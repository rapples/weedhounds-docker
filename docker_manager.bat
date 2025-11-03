@echo off
REM ====================================================================
REM Docker Dutchie Services Management Utility
REM ====================================================================

if "%1"=="" goto :show_usage

if "%1"=="start" goto :start_services
if "%1"=="stop" goto :stop_services
if "%1"=="restart" goto :restart_services
if "%1"=="status" goto :show_status
if "%1"=="logs" goto :show_logs
if "%1"=="build" goto :build_images
if "%1"=="clean" goto :clean_all
if "%1"=="deploy" goto :deploy_fresh

goto :show_usage

:start_services
echo 🚀 Starting Dutchie Docker services...
docker-compose up -d
goto :end

:stop_services
echo 🛑 Stopping Dutchie Docker services...
docker-compose down
goto :end

:restart_services
echo 🔄 Restarting Dutchie Docker services...
docker-compose restart
goto :end

:show_status
echo 📊 Dutchie Docker Services Status:
echo.
docker-compose ps
echo.
echo 🌐 Service Health Check:
curl -s http://localhost:8888/stats | findstr "service_status" 2>nul || echo "❌ Token Service not responding"
curl -s http://localhost:8889/ >nul 2>&1 && echo "✅ Browser Proxy responding" || echo "❌ Browser Proxy not responding"
curl -s http://localhost:8891/ >nul 2>&1 && echo "✅ GraphQL Proxy responding" || echo "❌ GraphQL Proxy not responding"
goto :end

:show_logs
if "%2"=="" (
    echo 📜 Showing logs for all services...
    docker-compose logs -f
) else (
    echo 📜 Showing logs for %2...
    docker-compose logs -f %2
)
goto :end

:build_images
echo 🔨 Building Docker images...
docker-compose build --no-cache
goto :end

:clean_all
echo 🧹 Cleaning up Docker resources...
echo ⚠️  This will remove all containers, images, and volumes!
set /p confirm="Are you sure? (y/N): "
if /i "%confirm%"=="y" (
    docker-compose down -v --rmi all --remove-orphans
    docker system prune -f
    echo ✅ Cleanup complete
) else (
    echo ❌ Cleanup cancelled
)
goto :end

:deploy_fresh
echo 🚀 Fresh deployment of Dutchie services...
docker-compose down --remove-orphans
docker-compose build --no-cache
docker-compose up -d
echo ⏳ Waiting for services to start...
timeout /t 30 /nobreak >nul
call :show_status
goto :end

:show_usage
echo.
echo ========================================
echo   🐳 DOCKER DUTCHIE SERVICES MANAGER
echo ========================================
echo.
echo Usage: %0 ^<command^>
echo.
echo Commands:
echo   start      - Start all services
echo   stop       - Stop all services  
echo   restart    - Restart all services
echo   status     - Show service status and health
echo   logs       - Show logs (optional: specify service name)
echo   build      - Rebuild Docker images
echo   clean      - Clean all Docker resources
echo   deploy     - Fresh deployment (stop, build, start)
echo.
echo Examples:
echo   %0 start
echo   %0 logs dutchie-token-service
echo   %0 status
echo   %0 deploy
echo.
echo Services:
echo   • dutchie-token-service    (Port 8888)
echo   • dutchie-browser-proxy    (Port 8889)
echo   • dutchie-graphql-proxy    (Port 8891)
echo   • dutchie-redis           (Port 6379)
echo   • dutchie-nginx           (Port 80)
echo.

:end