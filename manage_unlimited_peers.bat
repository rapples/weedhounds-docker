@echo off
REM WeedHounds Unlimited Peer-to-Peer Distributed Cache Management
REM This script manages the unlimited peer network system for cannabis data sharing

echo.
echo ========================================
echo WeedHounds Unlimited P2P Cache Manager
echo ========================================
echo.

if "%1"=="start" goto start
if "%1"=="stop" goto stop
if "%1"=="restart" goto restart
if "%1"=="status" goto status
if "%1"=="logs" goto logs
if "%1"=="peers" goto peers
if "%1"=="cache" goto cache
if "%1"=="health" goto health
if "%1"=="cleanup" goto cleanup
goto help

:start
echo Starting unlimited peer-to-peer distributed cache system...
docker-compose up -d
echo.
echo Waiting for services to initialize...
timeout /t 10 /nobreak >nul
echo.
echo Services started! Peer network is now accepting unlimited remote connections.
echo.
echo Available endpoints:
echo - DHT Coordinator: http://localhost:8000
echo - Cannabis Menu API: http://localhost:8000/cannabis/menu
echo - Terpene Analysis API: http://localhost:8000/cannabis/terpenes
echo - Peer Network Status: http://localhost:8000/peers/status
echo - Cache Statistics: http://localhost:8000/cache/stats
echo.
goto end

:stop
echo Stopping unlimited peer-to-peer system...
docker-compose down
echo System stopped.
goto end

:restart
echo Restarting unlimited peer-to-peer system...
docker-compose down
timeout /t 5 /nobreak >nul
docker-compose up -d
echo System restarted with fresh peer connections.
goto end

:status
echo.
echo === System Status ===
docker-compose ps
echo.
echo === Peer Network Health ===
curl -s http://localhost:8000/peers/status || echo "Coordinator not responding"
echo.
echo === Cache Statistics ===
curl -s http://localhost:8000/cache/stats || echo "Cache stats unavailable"
echo.
goto end

:logs
if "%2"=="" (
    echo Showing logs for all services...
    docker-compose logs --tail=50 -f
) else (
    echo Showing logs for %2...
    docker-compose logs --tail=50 -f %2
)
goto end

:peers
echo.
echo === Connected Peers ===
curl -s http://localhost:8000/peers/list | python -m json.tool 2>nul || echo "Unable to retrieve peer list"
echo.
echo === Peer Discovery Status ===
curl -s http://localhost:8000/peers/discovery | python -m json.tool 2>nul || echo "Discovery status unavailable"
echo.
goto end

:cache
echo.
echo === Cache Overview ===
curl -s http://localhost:8000/cache/overview | python -m json.tool 2>nul || echo "Cache overview unavailable"
echo.
echo === Storage Usage ===
curl -s http://localhost:8000/cache/storage | python -m json.tool 2>nul || echo "Storage stats unavailable"
echo.
echo === Recent Activity ===
curl -s http://localhost:8000/cache/activity | python -m json.tool 2>nul || echo "Activity log unavailable"
echo.
goto end

:health
echo.
echo === System Health Check ===
echo Checking coordinator...
curl -s http://localhost:8000/health > nul && echo "✓ Coordinator: Healthy" || echo "✗ Coordinator: Unhealthy"

echo Checking peer network...
curl -s http://localhost:8000/peers/health > nul && echo "✓ Peer Network: Healthy" || echo "✗ Peer Network: Unhealthy"

echo Checking distributed storage...
curl -s http://localhost:8000/storage/health > nul && echo "✓ Distributed Storage: Healthy" || echo "✗ Distributed Storage: Unhealthy"

echo Checking Redis coordination...
docker-compose exec -T redis redis-cli ping 2>nul | findstr PONG >nul && echo "✓ Redis: Healthy" || echo "✗ Redis: Unhealthy"

echo.
echo === Cannabis Data APIs ===
curl -s http://localhost:8000/cannabis/menu/test > nul && echo "✓ Menu API: Available" || echo "✗ Menu API: Unavailable"
curl -s http://localhost:8000/cannabis/terpenes/test > nul && echo "✓ Terpene API: Available" || echo "✗ Terpene API: Unavailable"
echo.
goto end

:cleanup
echo.
echo Performing system cleanup...
echo.
echo Cleaning expired cache entries...
curl -s -X POST http://localhost:8000/cache/cleanup || echo "Cleanup request failed"
echo.
echo Pruning disconnected peers...
curl -s -X POST http://localhost:8000/peers/prune || echo "Peer pruning failed"
echo.
echo Compacting storage...
curl -s -X POST http://localhost:8000/storage/compact || echo "Storage compaction failed"
echo.
echo Cleanup completed.
goto end

:help
echo.
echo Usage: %0 [command]
echo.
echo Commands:
echo   start     - Start the unlimited peer-to-peer system
echo   stop      - Stop all services
echo   restart   - Restart the system with fresh connections
echo   status    - Show system status and statistics
echo   logs      - Show service logs (optional: specify service name)
echo   peers     - Show connected peers and discovery status
echo   cache     - Show cache statistics and storage usage
echo   health    - Perform comprehensive health check
echo   cleanup   - Clean expired data and optimize storage
echo.
echo Examples:
echo   %0 start
echo   %0 logs dht-coordinator
echo   %0 peers
echo   %0 health
echo.
echo The system supports unlimited remote peer connections for cannabis data sharing.
echo Peers automatically discover each other and share cached data to reduce API calls.
echo.

:end