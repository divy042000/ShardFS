package server

import (
	"log"
)

type LeaderElector struct {
	hm *HeartbeatManager
}

func NewLeaderElector(hm *HeartbeatManager) *LeaderElector {
	return &LeaderElector{hm: hm}
}

func (le *LeaderElector) ElectLeader(
	chunkSize int64,
	servers []string,
	loads map[string]int64,
	spaces map[string]int64,
) string {
	if chunkSize <= 0 {
		log.Printf("[ElectLeader] Invalid input: chunkSize=%d Bytes", chunkSize)
		return ""
	}

	le.hm.mu.Lock()
	defer le.hm.mu.Unlock()

	log.Printf("[ElectLeader] Chunk size to allocate: %d Bytes", chunkSize)
	log.Printf("[ElectLeader] Candidate servers: %v", servers)

	// Try priority queue
	for _, item := range le.hm.pq {
		serverID := item.ServerID
		freeSpace, exists := spaces[serverID]
		if !exists {
			log.Printf("[ElectLeader] Server %s in pq but not in spaces", serverID)
			continue
		}

		log.Printf("[ElectLeader] Evaluating server %s | Free: %d Bytes | Score: %.2f", serverID, freeSpace, item.Score)

		if freeSpace < chunkSize {
			log.Printf("[ElectLeader] ❌ Skipping %s: not enough space (%d < %d)", serverID, freeSpace, chunkSize)
			continue
		}

		for _, s := range servers {
			if s == serverID && le.hm.chunkServers[serverID] != nil {
				log.Printf("[ElectLeader] ✅ Selected leader: %s from pq", serverID)
				return serverID
			}
		}
	}

	// Fallback to servers list
	log.Printf("[ElectLeader] No leader found in pq, checking servers list...")
	for _, serverID := range servers {
		freeSpace, exists := spaces[serverID]
		if !exists {
			log.Printf("[ElectLeader] Server %s not in spaces", serverID)
			continue
		}

		log.Printf("[ElectLeader] Evaluating server %s | Free: %d MB", serverID, freeSpace)

		if freeSpace < chunkSize {
			log.Printf("[ElectLeader] ❌ Skipping %s: not enough space (%d < %d)", serverID, freeSpace, chunkSize)
			continue
		}

		if le.hm.chunkServers[serverID] != nil {
			log.Printf("[ElectLeader] ✅ Selected leader: %s from servers list", serverID)
			return serverID
		}
	}

	log.Printf("[ElectLeader] ⚠️ No suitable leader found for chunkSize=%d MB, servers=%v, spaces=%v", chunkSize, servers, spaces)
	return ""
}
