// Command clavis starts a distributed lock service node. It supports
// bootstrapping a new cluster, joining an existing one, and removing nodes
// via CLI flags. The node runs a Raft consensus group and a gRPC lock service.
package main

import (
	"context"
	"flag"
	pb "github.com/Mfon-19/clavis/api/v1"
	"github.com/Mfon-19/clavis/internal/app"
	"github.com/google/uuid"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var (
		nodeID            = flag.String("node-id", "", "Unique node ID (generates UUID if empty)")
		raftAddr          = flag.String("raft-addr", "127.0.0.1:7000", "Raft bind address")
		raftAdvertiseAddr = flag.String("raft-advertise-addr", "", "Advertised Raft address for peer-to-peer cluster traffic")
		grpcAddr          = flag.String("grpc-addr", ":9000", "gRPC server address")
		grpcAdvertiseAddr = flag.String("grpc-advertise-addr", "", "Advertised gRPC address for clients and cluster redirects")
		dataDir           = flag.String("data-dir", "./data", "Data directory for Raft storage")
		bootstrap         = flag.Bool("bootstrap", false, "Bootstrap a new cluster")
		joinAddr          = flag.String("join", "", "Join an existing cluster through this node's gRPC address")
		removeNodeID      = flag.String("remove-node", "", "Remove a node from the cluster and exit")
		clusterAddr       = flag.String("cluster-addr", "", "Cluster gRPC address for administrative operations like --remove-node")
	)
	flag.Parse()

	if *bootstrap && *joinAddr != "" {
		log.Fatal("cannot use --bootstrap and --join together")
	}

	if *removeNodeID != "" {
		if *clusterAddr == "" {
			log.Fatal("--cluster-addr is required with --remove-node")
		}
		if err := app.RemoveClusterNode(context.Background(), *clusterAddr, *removeNodeID); err != nil {
			log.Fatalf("failed to remove node: %v", err)
		}
		log.Printf("Removed node %s from cluster via %s", *removeNodeID, *clusterAddr)
		return
	}

	var (
		nid uuid.UUID
		err error
	)
	if *nodeID == "" {
		nid = uuid.New()
		log.Printf("generated node id : %s", nid)
	} else {
		nid, err = uuid.Parse(*nodeID)
		if err != nil {
			log.Fatalf("invalid node id: %v", err)
		}
	}

	resolvedRaftAdvertiseAddr, err := app.DeriveRaftAdvertiseAddr(*raftAddr, *raftAdvertiseAddr, *grpcAdvertiseAddr)
	if err != nil {
		log.Fatalf("invalid raft advertise address: %v", err)
	}

	resolvedGRPCAdvertiseAddr, err := app.DeriveAdvertiseAddr(resolvedRaftAdvertiseAddr, *grpcAddr, *grpcAdvertiseAddr)
	if err != nil {
		log.Fatalf("invalid gRPC advertise address: %v", err)
	}

	log.Printf("Starting lowkey node...")
	log.Printf("  Node ID: %s", nid)
	log.Printf("  Raft: %s", *raftAddr)
	log.Printf("  Advertised Raft: %s", resolvedRaftAdvertiseAddr)
	log.Printf("  gRPC: %s", *grpcAddr)
	log.Printf("  Advertised gRPC: %s", resolvedGRPCAdvertiseAddr)
	log.Printf("  Data: %s", *dataDir)
	log.Printf("  Bootstrap: %v", *bootstrap)
	log.Printf("  Join: %s", *joinAddr)

	runtime, err := app.NewRuntime(app.Config{
		NodeID:            nid,
		RaftAddr:          *raftAddr,
		RaftAdvertiseAddr: resolvedRaftAdvertiseAddr,
		GRPCAddr:          *grpcAddr,
		GRPCAdvertiseAddr: resolvedGRPCAdvertiseAddr,
		DataDir:           *dataDir,
		Bootstrap:         *bootstrap,
	})
	if err != nil {
		log.Fatalf("failed to create runtime: %v", err)
	}
	defer runtime.Stop(context.Background())

	errCh := runtime.Start(context.Background())
	log.Printf("gRPC server listening on %s", *grpcAddr)

	if *joinAddr != "" {
		member := runtime.Member()
		if err := app.JoinCluster(context.Background(), *joinAddr, &pb.JoinNodeRequest{
			NodeId:      nid.String(),
			RaftAddress: member.RaftAddress,
			GrpcAddress: member.GRPCAddress,
		}); err != nil {
			log.Fatalf("failed to join cluster via %s: %v", *joinAddr, err)
		}
		log.Printf("Joined cluster via %s", *joinAddr)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	log.Println("OwO lowkey is ready")
	log.Println("  Press Ctrl+C to stop")

	select {
	case <-sigCh:
	case err := <-errCh:
		log.Fatalf("runtime failed: %v", err)
	}
	log.Println("\nShutting down gracefully...")

	if err := runtime.Stop(context.Background()); err != nil {
		log.Printf("shutdown error: %v", err)
	}

	log.Println(":} Shutdown complete")
}
