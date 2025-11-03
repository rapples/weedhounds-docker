#!/bin/bash

# Start Xvfb virtual display for Docker containers
if [ -f /.dockerenv ] || [ "$DOCKER_CONTAINER" = "true" ]; then
    echo "🖥️ Starting Xvfb virtual display for Docker container..."
    
    # Start Xvfb in the background
    Xvfb :99 -screen 0 1920x1080x24 -ac +extension GLX +render -noreset &
    XVFB_PID=$!
    
    # Set display environment variable
    export DISPLAY=:99
    
    # Wait for Xvfb to be ready
    sleep 3
    
    echo "✅ Xvfb started with PID $XVFB_PID on display :99"
    
    # Function to cleanup Xvfb on exit
    cleanup() {
        echo "🛑 Stopping Xvfb..."
        kill $XVFB_PID 2>/dev/null
        exit
    }
    
    # Trap signals to cleanup
    trap cleanup SIGTERM SIGINT EXIT
fi

# Start the Python service
echo "🚀 Starting Dutchie Token Service..."
exec "$@"