package queryservice

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
)

func handleLeakConfig(ctx context.Context, req *pb.Request, resp *pb.Response) error {
	if req.CoreDumpConfig == nil {
		return nil
	}
	switch strings.ToLower(req.CoreDumpConfig.Action) {
	case "enable":
		mpool.Enable = true
	case "disable":
		mpool.Enable = false
	}
	resp.CoreDumpConfig = &pb.CoreDumpConfigResponse{}
	return nil
}
