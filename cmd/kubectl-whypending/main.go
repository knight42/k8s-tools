package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	labelutil "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
)

func getProgramName() string {
	name := filepath.Base(os.Args[0])
	if strings.HasPrefix(name, "kubectl-") {
		return "kubectl whypending"
	}
	return name
}

type Options struct {
	getter       *genericclioptions.ConfigFlags
	nodeSelector string
}

func listPodsOnNode(ctx context.Context, cs kubernetes.Interface, nodeName string) ([]corev1.Pod, error) {
	podLst, err := cs.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", nodeName).String(),
	})
	if err != nil {
		return nil, fmt.Errorf("list pods on node %s: %w", nodeName, err)
	}
	return podLst.Items, nil
}

func convertOp(in corev1.NodeSelectorOperator) selection.Operator {
	switch in {
	case corev1.NodeSelectorOpIn:
		return selection.In
	case corev1.NodeSelectorOpNotIn:
		return selection.NotIn
	case corev1.NodeSelectorOpExists:
		return selection.Exists
	case corev1.NodeSelectorOpDoesNotExist:
		return selection.DoesNotExist
	case corev1.NodeSelectorOpGt:
		return selection.GreaterThan
	case corev1.NodeSelectorOpLt:
		return selection.LessThan
	default:
		return ""
	}
}

func addUpResourceList(dst, other corev1.ResourceList) {
	for name, quantity := range other {
		cur := dst[name]
		cur.Add(quantity)
		dst[name] = cur
	}
}

func getResourceRequestsOfPod(podSpec *corev1.PodSpec) corev1.ResourceList {
	reqs := make(corev1.ResourceList)
	for _, container := range podSpec.InitContainers {
		for name, quantity := range container.Resources.Requests {
			cur := reqs[name]
			if cur.Cmp(quantity) < 0 {
				reqs[name] = quantity
			}
		}
	}

	for _, container := range podSpec.Containers {
		addUpResourceList(reqs, container.Resources.Requests)
	}

	return reqs
}

func reason(cs kubernetes.Interface, pod *corev1.Pod, node *corev1.Node) error {
	// unschedulable
	if node.Spec.Unschedulable {
		return fmt.Errorf("node %s is unschedulable", node.Name)
	}

	// node selectors
	if len(pod.Spec.NodeSelector) > 0 {
		nodeLabels := labelutil.Set(node.Labels)
		sel := labelutil.Set(pod.Spec.NodeSelector).AsSelector()
		if !sel.Matches(nodeLabels) {
			return fmt.Errorf("node %s does not match nodeSelector", node.Name)
		}
	}

	// taints and tolerations
	for _, taint := range node.Spec.Taints {
		tolerated := false
		for _, toleration := range pod.Spec.Tolerations {
			if len(toleration.Key) == 0 && toleration.Operator == corev1.TolerationOpExists {
				tolerated = true
				break
			}
			if toleration.Key == taint.Key {
				if toleration.Operator == corev1.TolerationOpExists {
					tolerated = true
					break
				}
				if toleration.Value == taint.Value {
					tolerated = true
					break
				}
			}
		}
		if !tolerated {
			return fmt.Errorf("node %s has untolerated taint %s=%s", node.Name, taint.Key, taint.Value)
		}
	}

	podAffinity := pod.Spec.Affinity
	if podAffinity != nil {
		// node affinity
		if podAffinity.NodeAffinity != nil && podAffinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			nodeLabels := labelutil.Set(node.Labels)
			matched := false
			for _, selTerm := range podAffinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
				nodeLabelsSelector := labelutil.NewSelector()
				for _, expr := range selTerm.MatchExpressions {
					r, err := labelutil.NewRequirement(expr.Key, convertOp(expr.Operator), expr.Values)
					if err != nil {
						return fmt.Errorf("convert expr to requirement: %s: %w", expr, err)
					}
					nodeLabelsSelector = nodeLabelsSelector.Add(*r)
				}
				if nodeLabelsSelector.Matches(nodeLabels) {
					matched = true
					break
				}
				// FIXME: check MatchFields
			}
			if !matched {
				return fmt.Errorf("node %s does not match required nodeAffinity", node.Name)
			}
		}

		// pod affinity and anti-affinity
	}

	// resources
	reqResources := getResourceRequestsOfPod(&pod.Spec)

	otherPods, err := listPodsOnNode(context.Background(), cs, node.Name)
	if err != nil {
		return err
	}
	for _, p := range otherPods {
		addUpResourceList(reqResources, getResourceRequestsOfPod(&p.Spec))
	}
	for name, quantity := range reqResources {
		availRes, ok := node.Status.Allocatable[name]
		if !ok {
			return fmt.Errorf("node %s does not have resource %s", node.Name, name)
		}
		if availRes.Cmp(quantity) < 0 {
			return fmt.Errorf("node %s does not have enough resource %s: have %s, need %s", node.Name, name, availRes.String(), quantity.String())
		}
	}

	// persistent volume

	return nil
}

func (o *Options) Run(ctx context.Context, args []string) error {
	ns, _, err := o.getter.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return fmt.Errorf("get namespace: %w", err)
	}
	restCfg, err := o.getter.ToRESTConfig()
	if err != nil {
		return fmt.Errorf("to REST config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("new clientset: %w", err)
	}

	pod, err := cs.CoreV1().Pods(ns).Get(ctx, args[0], metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get pod: %w", err)
	}
	if pod.Status.Phase != corev1.PodPending {
		return fmt.Errorf("pod is not pending")
	}

	var nodeList []corev1.Node
	nodeNames := args[1:]
	if len(nodeNames) > 0 {
		for _, name := range nodeNames {
			node, err := cs.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("get node %s: %w", name, err)
			}
			nodeList = append(nodeList, *node)
		}
	} else if o.nodeSelector != "" {
		nodes, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{
			LabelSelector: o.nodeSelector,
		})
		if err != nil {
			return fmt.Errorf("list nodes: %w", err)
		}
		nodeList = nodes.Items
	} else {
		nodes, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("list nodes: %w", err)
		}
		nodeList = nodes.Items
	}

	for _, node := range nodeList {
		err := reason(cs, pod, &node)
		if err != nil {
			fmt.Printf("Node %s: %v\n", node.Name, err)
			continue
		}
		fmt.Printf("Node %s: OK\n", node.Name)
	}
	return nil
}

func main() {
	cf := genericclioptions.NewConfigFlags(true)
	o := Options{
		getter: cf,
	}
	c := cobra.Command{
		Use:  fmt.Sprintf(`%s [options] POD_NAME [...NODE_NAME]`, getProgramName()),
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.Run(cmd.Context(), args)
		},
		SilenceUsage: true,
	}
	flags := c.Flags()
	flags.SortFlags = false
	flags.StringVar(cf.KubeConfig, "kubeconfig", *cf.KubeConfig, "Path to the kubeconfig file to use for CLI requests.")
	flags.StringVarP(cf.Namespace, "namespace", "n", *cf.Namespace, "If present, the namespace scope for this CLI request")
	flags.StringVar(cf.Context, "context", *cf.Context, "The name of the kubeconfig context to use")
	flags.StringVar(cf.ClusterName, "cluster", *cf.ClusterName, "The name of the kubeconfig cluster to use")
	flags.StringVar(&o.nodeSelector, "node-selector", "", "A selector to restrict the nodes to check")

	_ = c.Execute()
}
