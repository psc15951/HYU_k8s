package testscore

import (
	"context"
    	"time"
	"fmt"

    	"github.com/prometheus/client_golang/api"
    	prov1 "github.com/prometheus/client_golang/api/prometheus/v1"
    	"github.com/prometheus/common/model"
    	"k8s.io/api/core/v1"
    	"k8s.io/apimachinery/pkg/runtime"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
    	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
    	"k8s.io/kubernetes/pkg/scheduler/framework"
    	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

type TestScore struct{
	handle		framework.Handle
	prometheusAPI	prov1.API
}

var _ framework.ScorePlugin = &TestScore{}

const (
	Name = names.TestScore
)
// Plugin name을 반환하는 메서드
func (pl *TestScore) Name() string {
	return Name
}

// Prometheus에서 트래픽 데이터를 가져오는 메서드
func (pl *TestScore) getNodeTraffic(nodeName string) (float64, error) {
    	query := fmt.Sprintf( `sum(rate(istio_request_bytes_sum{destination_workload_namespace=~".*"}[1m])) by (destination_workload, destination_workload_namespace)`)

    	result, warnings, err := pl.prometheusAPI.Query(context.Background(), query, time.Now())
    	if err != nil {
        	klog.Errorf("Error querying Prometheus: %v", err)
        	return 0, err
    	}
    	if len(warnings) > 0 {
        	klog.Warningf("Prometheus query warnings: %v", warnings)
    	}

        if result.Type() != model.ValVector {
            klog.Errorf("Unexpected result format for query: %s", query)
            return 0, fmt.Errorf("unexpected result format: %s", result.Type().String())
        }

    	vector := result.(model.Vector)

	var totalTraffic float64
    	for _, sample := range vector {
        	namespace := string(sample.Metric["destination_workload_namespace"])
        	workload := string(sample.Metric["destination_workload"])

        	if isPodOnNode(pl.handle.ClientSet(), namespace, workload, nodeName) {
            		totalTraffic += float64(sample.Value)
        	}
    	}

    	return totalTraffic, nil
}

func isPodOnNode(clientset kubernetes.Interface, namespace, workload, nodeName string) bool {
	pods, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=%s", workload),
	})
	if err != nil {
		klog.Errorf("Error listing pods in namespace %s with workload %s: %v", namespace, workload, err)
		return false
	}

        for _, pod := range pods.Items {
		if pod.Spec.NodeName == nodeName {
			return true
		}
	}

	return false
}

// 스코어링 플러그인의 구현
func (pl *TestScore) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	traffic, err := pl.getNodeTraffic(nodeName)
    	if err != nil {
        	return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error getting node traffic: %v", err))
    	}

    	// 트래픽 양이 적을수록 높은 점수를 줌
    	score := int64(1000000 / (traffic + 1)) // traffic이 0이면 최고 점수, 많을수록 낮은 점수
	klog.V(2).Infof("%s score: %d",nodeName,score)
    	return score, framework.NewStatus(framework.Success)
}

// 스코어 정규화 로직
func (pl *TestScore) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	var maxScore int64
    	for _, score := range scores {
        	if score.Score > maxScore {
            	maxScore = score.Score
		}
    	}
    	for i := range scores {
        	scores[i].Score = scores[i].Score * framework.MaxNodeScore / maxScore
    	}
    	return framework.NewStatus(framework.Success)
}

func (pl *TestScore) ScoreExtensions() framework.ScoreExtensions {
        return pl
}

// SimpleScorePlugin의 인스턴스를 생성하고 반환
func New(_ context.Context, plArgs runtime.Object, h framework.Handle) (framework.Plugin, error) {
	args, err := getArgs(plArgs)
    	if err != nil {
        	return nil, err
    	}

    	client, err := api.NewClient(api.Config{
        	Address: args.PrometheusAddress,
    	})
    	if err != nil {
        	return nil, fmt.Errorf("failed to create Prometheus client: %w", err)
    	}

    	prometheusAPI := prov1.NewAPI(client)

    	return &TestScore{
        	handle:        h,
        	prometheusAPI: prometheusAPI,
    	}, nil
}

func getArgs(obj runtime.Object) (config.TestScoreArgs, error) {
	ptr, ok := obj.(*config.TestScoreArgs)
	if !ok {
		return config.TestScoreArgs{}, fmt.Errorf("args are not of type TestScoreArgs, got %T", obj)
	}
	return *ptr, nil
}
