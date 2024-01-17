package ctl

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleLeak(proc *process.Process, service serviceType,
	param string, sender requestSender) (Result, error) {
	var addrs []string

	qs := proc.QueryService
	mc := clusterservice.GetMOCluster()
	if service == cn {
		mc.GetCNService(
			clusterservice.NewSelector(),
			func(c metadata.CNService) bool {
				addrs = append(addrs, c.QueryAddress)
				return true
			})
	}
	if service == tn {
		mc.GetTNService(
			clusterservice.NewSelector(),
			func(d metadata.TNService) bool {
				if d.QueryAddress != "" {
					addrs = append(addrs, d.QueryAddress)
				}
				return true
			})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	for _, addr := range addrs {
		req := qs.NewRequest(querypb.CmdMethod_CoreDumpConfig)
		req.CoreDumpConfig = &querypb.CoreDumpConfigRequest{Action: param}
		resp, err := qs.SendMessage(ctx, addr, req)
		if err != nil {
			return Result{}, err
		}
		qs.Release(resp)
	}
	return Result{
		Method: LeakMethod,
		Data:   "OK",
	}, nil
}
