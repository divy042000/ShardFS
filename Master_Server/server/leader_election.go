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
    
    le.hm.mu.Lock()
	defer le.hm.mu.Unlock()
     
    // check top servers from priority queue

	for _, item := range le.hm.pq {
    server := item.ServerID
	available := item.FreeSpace
	if available >= chunkSize {
        for _, s := range servers{
			if s == server && le.hm.chunkServers[server] != nil{
				return server
			}
		}
	}
	}
	return ""  // No suitable server found
}
