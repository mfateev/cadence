// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package worker

import (
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/cassandra"
	"github.com/uber/cadence/common/persistence/sql"
	"github.com/uber/cadence/common/service"

	sc "github.com/uber/cadence/service"
)

type (
	// Service represents the cadence-worker service.  This service host all background processing which needs to happen
	// for a Cadence cluster.  This service runs the replicator which is responsible for applying replication tasks
	// generated by remote clusters.
	Service struct {
		stopC         chan struct{}
		params        *service.BootstrapParams
		config        *Config
		metricsClient metrics.Client
	}

	// Config contains all the service config for worker
	Config struct {
		// Replicator settings
		ReplicatorConcurrency      int
		ReplicatorBufferRetryCount int
		ReplicationTaskMaxRetry    int
	}
)

// NewService builds a new cadence-worker service
func NewService(params *service.BootstrapParams) common.Daemon {
	return &Service{
		params: params,
		config: NewConfig(),
		stopC:  make(chan struct{}),
	}
}

// NewConfig builds the new Config for cadence-worker service
func NewConfig() *Config {
	return &Config{
		ReplicatorConcurrency:      1000,
		ReplicatorBufferRetryCount: 8,
		ReplicationTaskMaxRetry:    5,
	}
}

// Start is called to start the service
func (s *Service) Start() {
	p := s.params
	base := service.New(p)

	log := base.GetLogger()
	log.Infof("%v starting", common.WorkerServiceName)
	base.Start()

	s.metricsClient = base.GetMetricsClient()

	var metadataManager persistence.MetadataManager
	var err error
	if sc.UseMysql {
		metadataManager, err = sql.NewMetadataPersistence("uber",
			"uber",
			"localhost",
			"3306",
			"catalyst_test")
	} else {

		// worker only use the v2
		metadataManager, err = cassandra.NewMetadataPersistenceV2(p.CassandraConfig.Hosts,
			p.CassandraConfig.Port,
			p.CassandraConfig.User,
			p.CassandraConfig.Password,
			p.CassandraConfig.Datacenter,
			p.CassandraConfig.Keyspace,
			p.ClusterMetadata.GetCurrentClusterName(),
			p.Logger)
	}

	if err != nil {
		log.Fatalf("failed to create metadata manager: %v", err)
	}
	metadataManager = persistence.NewMetadataPersistenceClient(metadataManager, base.GetMetricsClient(), log)

	history, err := base.GetClientFactory().NewHistoryClient()
	if err != nil {
		log.Fatalf("failed to create history service client: %v", err)
	}

	replicator := NewReplicator(p.ClusterMetadata, metadataManager, history, s.config, p.MessagingClient, log,
		s.metricsClient)
	if err := replicator.Start(); err != nil {
		replicator.Stop()
		log.Fatalf("Fail to start replicator: %v", err)
	}

	log.Infof("%v started", common.WorkerServiceName)
	<-s.stopC
	base.Stop()
}

// Stop is called to stop the service
func (s *Service) Stop() {
	select {
	case s.stopC <- struct{}{}:
	default:
	}
	s.params.Logger.Infof("%v stopped", common.WorkerServiceName)
}
