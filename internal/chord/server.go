package chord

import (
	"context"
	"math/big"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "chord/gen/chordpb"
)

type Server struct {
	pb.UnimplementedRingServiceServer
	pb.UnimplementedFileServiceServer
	n *Node
}

func Register(s *grpc.Server, n *Node) {
	srv := &Server{n: n}
	pb.RegisterRingServiceServer(s, srv)
	pb.RegisterFileServiceServer(s, srv)
}

func unavailable(err error) error { return status.Error(codes.Unavailable, err.Error()) }

// RingService

func (s *Server) FindSuccessor(ctx context.Context, req *pb.FindSuccessorRequest) (*pb.FindSuccessorResponse, error) {
	successor, path, err := s.n.FindSuccessor(ctx, IDFromBytes(req.KeyId), int(req.HopCount))
	if err != nil {
		return nil, unavailable(err)
	}
	return &pb.FindSuccessorResponse{Successor: successor.pb(), Path: path}, nil
}

func (s *Server) GetPredecessor(ctx context.Context, _ *pb.Empty) (*pb.GetPredecessorResponse, error) {
	p := s.n.predecessor()
	if p == nil {
		return &pb.GetPredecessorResponse{}, nil
	}
	return &pb.GetPredecessorResponse{HasPredecessor: true, Predecessor: p.pb()}, nil
}

func (s *Server) Notify(ctx context.Context, req *pb.NotifyRequest) (*pb.Empty, error) {
	s.n.notify(refFromPB(req.Node))
	return &pb.Empty{}, nil
}

func (s *Server) Ping(ctx context.Context, _ *pb.Empty) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

// TransferKeys streams every stored file that no longer belongs here once new_node
// has joined as our predecessor, i.e. keys outside (new_node, self], then deletes them.
func (s *Server) TransferKeys(req *pb.TransferKeysRequest, stream pb.RingService_TransferKeysServer) error {
	n := s.n
	newNode := refFromPB(req.NewNode)
	keys, err := n.store.Keys()
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	var moved []*big.Int
	for _, k := range keys {
		if inHalfOpen(k, newNode.ID, n.self.ID) {
			continue
		}
		data, found, err := n.store.Get(k)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		if !found {
			continue
		}
		if err := stream.Send(&pb.FileRecord{KeyId: IDBytes(k), Data: data}); err != nil {
			return err
		}
		moved = append(moved, k)
	}
	for _, k := range moved {
		n.store.Delete(k)
	}
	n.log.Info("keys transferred", "component", "ring", "to", newNode.Addr, "count", len(moved))
	return nil
}

func (s *Server) StoreLocal(ctx context.Context, req *pb.StoreLocalRequest) (*pb.Empty, error) {
	if err := s.n.store.Put(IDFromBytes(req.KeyId), req.Data); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.Empty{}, nil
}

func (s *Server) FetchLocal(ctx context.Context, req *pb.FetchLocalRequest) (*pb.FetchLocalResponse, error) {
	r, err := s.n.fetchLocal(IDFromBytes(req.KeyId))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.FetchLocalResponse{Responsible: r.responsible, Found: r.found, Data: r.data}, nil
}

// FileService

func (s *Server) PutFile(ctx context.Context, req *pb.PutFileRequest) (*pb.PutFileResponse, error) {
	r, err := s.n.PutFile(ctx, req.Name, req.Data)
	if err != nil {
		return nil, unavailable(err)
	}
	return &pb.PutFileResponse{Owner: r.Owner.pb(), HopCount: int32(len(r.Path) - 1), Path: r.Path, ElapsedNs: r.Elapsed.Nanoseconds()}, nil
}

func (s *Server) GetFile(ctx context.Context, req *pb.GetFileRequest) (*pb.GetFileResponse, error) {
	r, err := s.n.GetFile(ctx, req.Name, req.BypassCache)
	if err != nil {
		return nil, unavailable(err)
	}
	return &pb.GetFileResponse{
		Found:     r.Found,
		Data:      r.Data,
		Owner:     r.Owner.pb(),
		HopCount:  int32(len(r.Path) - 1),
		Path:      r.Path,
		FromCache: r.FromCache,
		ElapsedNs: r.Elapsed.Nanoseconds(),
		TraceId:   TraceID(ctx),
	}, nil
}

func (s *Server) GetNodeState(ctx context.Context, _ *pb.Empty) (*pb.NodeState, error) {
	n := s.n
	keys, err := n.store.Keys()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	st := &pb.NodeState{
		Self:         n.self.pb(),
		Successor:    n.successor().pb(),
		FileCount:    int64(len(keys)),
		CacheEnabled: n.cache.Enabled(),
		CacheHits:    n.cache.hits.Load(),
		CacheMisses:  n.cache.misses.Load(),
		CacheSize:    int64(n.cache.Size()),
	}
	if p := n.predecessor(); p != nil {
		st.HasPredecessor = true
		st.Predecessor = p.pb()
	}
	return st, nil
}

func (s *Server) FlushCache(ctx context.Context, _ *pb.Empty) (*pb.Empty, error) {
	s.n.cache.Flush()
	return &pb.Empty{}, nil
}
