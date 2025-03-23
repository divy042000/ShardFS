package server


type LeaderElector struct {
hm *HeartbeatManager      // ✅ HeartbeatManager Dependency	    
}

func NewLeaderElector(hm *HeartbeatManager) *LeaderElector {
	return &LeaderElector{hm:hm}
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
