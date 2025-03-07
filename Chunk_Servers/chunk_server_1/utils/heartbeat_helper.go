package utils

import (
	"fmt"
	"log"
	"os"
	"syscall"
	"time"

	"github.com/shirou/gopsutil/cpu"
//	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
)

// GetFreeDiskSpace returns the available disk space (in MB)
func GetFreeDiskSpace(storagePath string) int64 {
	var stat syscall.Statfs_t
	err := syscall.Statfs(storagePath, &stat)
	if err != nil {
		log.Printf("⚠️ Error retrieving disk space: %v", err)
		return -1
	}
	// Convert blocks to MB
        return int64 ((stat.Bavail * uint64(stat.Bsize)) / (1024 * 1024))
}

func GetTotalDiskSpace(storagePath string) int64{
        var stat syscall.Statfs_t
        err := syscall.Statfs(storagePath,&stat)
        if err != nil{
        log.Printf("⚠️ Error retrieving disk space: %v",err)
        return -1
        }
        return int64((stat.Blocks*uint64(stat.Bsize))/(1024*1024))
}


// GetCPUUsage returns the CPU usage percentage
func GetCPUUsage() float64 {
	percentages, err := cpu.Percent(time.Second, false)
	if err != nil {
		log.Printf("⚠️ Error retrieving CPU usage: %v", err)
		return -1
	}
	return percentages[0] // CPU usage for all cores
}

// GetMemoryUsage returns the memory usage percentage
func GetMemoryUsage() float64 {
	v, err := mem.VirtualMemory()
	if err != nil {
		log.Printf("⚠️ Error retrieving memory usage: %v", err)
		return -1
	}
	return v.UsedPercent
}

// GetNetworkUsage returns the network traffic data (sent + received in KB/s)
func GetNetworkUsage() float64 {
	ioStats, err := net.IOCounters(false)
	if err != nil {
		log.Printf("⚠️ Error retrieving network usage: %v", err)
		return -1
	}
	// Convert bytes to kilobytes
	return float64(ioStats[0].BytesRecv+ioStats[0].BytesSent) / 1024.0
}

// GetSystemLoad returns the system load average (1-minute)
func GetSystemLoad() float64 {
	loadAvg, err := load.Avg()
	if err != nil {
		log.Printf("⚠️ Error retrieving system load: %v", err)
		return -1
	}
	return loadAvg.Load1
}

// ValidateStoragePath ensures the storage path exists, otherwise creates it
func ValidateStoragePath(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Printf("ℹ️ Storage path %s does not exist. Creating...", path)
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return fmt.Errorf("❌ Failed to create storage path: %w", err)
		}
	}
	return nil
}

