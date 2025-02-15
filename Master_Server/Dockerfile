# **Step 1: Build Stage - Compiling Go Binary**
FROM golang:1.22 AS builder  
WORKDIR /app  

# **Cache dependencies (faster builds)**
COPY go.mod go.sum ./  
RUN go mod download  

# **Copy only source code (so it doesn't invalidate the cache)**
COPY . .  

# **Build the Go binary**
RUN go build -o masterserver .  

# **Step 2: Final Minimal Runtime Image**
FROM alpine:latest  
WORKDIR /app  

# **Copy built binary from builder stage (keeps final image small)**
COPY --from=builder /app/masterserver .  

# **Ensure binary is executable**
RUN chmod +x masterserver  

# **Expose the gRPC port**
EXPOSE 50052  

# **Run the Master Server**
CMD ["./masterserver"]
