package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/Jordation/godq"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Server struct {
	Cfg *Config
	log logrus.FieldLogger
	godq.UnimplementedGoDqServer
}

type Config struct {
	port int
	opts []grpc.ServerOption
}

func DefaultConfig() *Config {
	return &Config{
		port: 50051,
		opts: []grpc.ServerOption{
			grpc.Creds(insecure.NewCredentials()),
		},
	}
}

func New(ctx context.Context, cfg *Config) (*Server, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	s := &Server{
		log: logrus.New().WithField("svc", "server"),
		Cfg: cfg,
	}

	return s, nil
}

func (s *Server) Serve() (chan os.Signal, func()) {
	grpcSrv := grpc.NewServer(s.Cfg.opts...)
	godq.RegisterGoDqServer(grpcSrv, s)

	notiChan := make(chan os.Signal, 1)
	signal.Notify(notiChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Cfg.port))
		if err != nil {
			panic(err)
		}
		s.log.Error(grpcSrv.Serve(lis))
	}()

	return notiChan, func() {
		s.log.Info("shutting down")
		grpcSrv.GracefulStop()
	}
}

func (s *Server) ConsumeBatch(req *godq.ConsumeRequest, srv godq.GoDq_ConsumeBatchServer) error {
	for i := int32(0); i < req.BatchSize; i++ {
		if err := srv.Send(&godq.Message{
			Data: []byte(fmt.Sprintf("message number %d!", i)),
		}); err != nil {
			s.log.WithError(err).Error("error sending message")
			continue
		}
	}

	srv.SetTrailer(metadata.New(map[string]string{
		"next_offset": fmt.Sprintf("%d", req.Offset+int64(req.BatchSize)),
	}))

	return nil
}
