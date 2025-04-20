package server

import (
	"log"
	"sort"
)

// LeaderElector handles leader and replica selection
type LeaderElector struct {
	hm *HeartbeatManager
}

// NewLeaderElector creates a LeaderElector
func NewLeaderElector(hm *HeartbeatManager) *LeaderElector {
	return &LeaderElector{hm: hm}
}

// ElectLeader selects a leader for a chunk
func (le *LeaderElector) ElectLeader(
	chunkSize int64,
	servers []string,
	loads map[string]int64,
	spaces map[string]int64,
	
) string {
	if chunkSize <= 0 {
		log.Printf("⚠️ Invalid chunkSize: %d bytes", chunkSize)
		return ""
	}

	le.hm.mu.Lock()
	defer le.hm.mu.Unlock()
	log.Printf("🔒 Acquired lock for leader election, chunkSize=%d bytes", chunkSize)

	log.Printf("📋 Evaluating %d servers: %v", len(servers), servers)
	log.Printf("📋 Priority queue size: %d", len(le.hm.pq))
	log.Printf("📋 Starting leader election loop, pq size: %d", len(le.hm.pq))
	for i, item := range le.hm.pq {
		log.Printf("🔍 Iteration %d: ServerID=%s, Score=%.3f, FreeSpace=%d", i, item.ServerID, item.Score, item.FreeSpace)
		serverID := item.ServerID
		if !contains(servers, serverID) {
			log.Printf("⚠️ Skipping %s: not in candidate list", serverID)
			continue
		}
		// if !le.hm.IsChunkServerActive(serverID) {
		// 	log.Printf("⚠️ Skipping %s: not active (no heartbeat)", serverID)
		// 	continue
		// }
		log.Printf("✅ Evaluating %s: score=%.3f, freeSpace=%d bytes", serverID, item.Score, spaces[serverID])
		if spaces[serverID] < chunkSize {
			log.Printf("❌ Skipping %s: freeSpace=%d < chunkSize=%d", serverID, spaces[serverID], chunkSize)
			continue
		}
		log.Printf("✅ Selected leader: %s, score=%.3f, freeSpace=%d bytes", serverID, item.Score, spaces[serverID])
		return serverID
	}
	log.Printf("⚠️ No leader found for chunkSize=%d bytes", chunkSize)
	return ""
}

// SelectReplicas selects replica servers
func (le *LeaderElector) SelectReplicas(
	leaderID string,
	count int,
	servers []string,
	chunkSize int64,
	spaces map[string]int64,
) []string {
	le.hm.mu.Lock()
	defer le.hm.mu.Unlock()
	log.Printf("🔒 Acquired lock for replica selection, leader=%s, count=%d", leaderID, count)

	log.Printf("📋 Selecting %d replicas for chunkSize=%d bytes, excluding %s", count, chunkSize, leaderID)
	candidates := make([]*ServerScore, 0, len(le.hm.pq))
	for _, item := range le.hm.pq {
		serverID := item.ServerID
		if serverID == leaderID {
			log.Printf("⚠️ Skipping %s: is leader", serverID)
			continue
		}
		if !contains(servers, serverID) {
			log.Printf("⚠️ Skipping %s: not in candidate list", serverID)
			continue
		}
		// if !le.hm.IsChunkServerActive(serverID) {
		// 	log.Printf("⚠️ Skipping %s: not active (no heartbeat)", serverID)
		// 	continue
		// }
		if spaces[serverID] < chunkSize {
			log.Printf("❌ Skipping %s: freeSpace=%d < chunkSize=%d", serverID, spaces[serverID], chunkSize)
			continue
		}
		log.Printf("✅ Candidate %s: score=%.3f, freeSpace=%d bytes", serverID, item.Score, item.FreeSpace)
		candidates = append(candidates, item)
	}

	// Sort by score (ascending to prefer mid-range)
	log.Printf("📊 Sorting %d candidates by score", len(candidates))
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score < candidates[j].Score
	})

	replicas := make([]string, 0, count)
	for _, item := range candidates {
		if len(replicas) >= count {
			break
		}
		log.Printf("✅ Selected replica: %s, score=%.3f, freeSpace=%d bytes", item.ServerID, item.Score, item.FreeSpace)
		replicas = append(replicas, item.ServerID)
	}

	if len(replicas) < count {
		log.Printf("⚠️ Only found %d/%d replicas", len(replicas), count)
	}

	log.Printf("🔓 Released lock for replica selection")
	return replicas
}

// contains checks if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
