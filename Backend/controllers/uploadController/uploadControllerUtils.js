
function getProgress(processedChunks, totalChunks) {
    const progress = Math.round((processedChunks / totalChunks) * 100);
    const remainingChunks = totalChunks - processedChunks;
  
    return {
      progress: `${progress}%`,
      remainingChunks,
    };
  }
  
  module.exports = getProgress;
  