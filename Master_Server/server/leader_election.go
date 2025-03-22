package server

import "master_server/heartbeat_manager"

type LeaderElector struct {
	    
}

func NewLeaderElector() *LeaderElector {
	return &LeaderElector{}
}

func (le *LeaderElector) ElectLeader(totalSize int64, chunkCount int32, servers []string, loads map[string]int64, spaces map[string]int64) string {
	if chunkCount == 0 {
		return ""
	}

	chunkSize := totalSize / int64(chunkCount)
	if chunkSize < 64*1024*1024 {
		chunkSize = 64 * 1024 * 1024
	}

	computePower := make(map[string]float64)
	maxComputePower := float64(0)
	for _, server := range servers {
		computePower[server] = 100.0
		if info, exits := heartbeatManager.chunkServers[server]; exists {
			computePower[server] = 100.0
			if info, exists := heartbeatManager.chunkServers[server]; exists {
				computePower[server] = 100.0 - info.CpuUsage // Lower CPU usage is better
			}
		}
		if computePower[server] > maxComputePower {
			maxComputePower = computePower[server]
		}
	}
	if maxComputePower == 0 {
		maxComputePower = 1
	}

	bestServer := ""
	bestScore := float64(-1)
	for _, server := range servers {
      available := spaces[server] - loads[server]
	  if available < chunkSize {
		  continue
	  }

	  spaceScore := float64(available-chunkSize) / float64(spaces[server])
	  if spaceScore < 0 {
		  spaceScore = 0
	  }

	  computeScore := computePower[server] / maxComputePower
	  score := 0.6*spaceScore + 0.4*computeScore

	  if score > bestScore {
		  bestScore = score
		  bestServer = server
	  }
	}
	return bestServer
}
