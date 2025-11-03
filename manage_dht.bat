@echo off
REM WeedHounds DHT Docker Management Script for Windows

title WeedHounds DHT Manager

echo.
echo ==============================================
echo    WeedHounds DHT Docker Manager
echo ==============================================
echo.

if "%1"=="" goto help
if "%1"=="help" goto help
if "%1"=="build" goto build
if "%1"=="start" goto start
if "%1"=="stop" goto stop
if "%1"=="restart" goto restart
if "%1"=="logs" goto logs
if "%1"=="test" goto test
if "%1"=="health" goto health
if "%1"=="cleanup" goto cleanup
if "%1"=="deploy" goto deploy
goto help

:build
echo Building DHT services...
docker-compose build
if %errorlevel% neq 0 (
    echo ERROR: Build failed
    exit /b 1
)
echo Build complete!
goto end

:start
echo Starting DHT services...
docker-compose up -d
if %errorlevel% neq 0 (
    echo ERROR: Start failed
    exit /b 1
)
echo Services started!
echo.
echo Service Status:
docker-compose ps
goto end

:stop
echo Stopping DHT services...
docker-compose down
if %errorlevel% neq 0 (
    echo ERROR: Stop failed
    exit /b 1
)
echo Services stopped!
goto end

:restart
echo Restarting DHT services...
docker-compose down
docker-compose up -d
if %errorlevel% neq 0 (
    echo ERROR: Restart failed
    exit /b 1
)
echo Services restarted!
goto end

:logs
if "%2"=="" (
    echo Showing all service logs...
    docker-compose logs -f
) else (
    echo Showing logs for %2...
    docker-compose logs -f %2
)
goto end

:test
echo Running DHT system tests...
timeout /t 5 /nobreak > nul
python test_dht_system.py
goto end

:health
echo Checking service health...
echo.
curl -s http://localhost:9000/health > nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] DHT Coordinator ^(port 9000^) - Healthy
) else (
    echo [ERROR] DHT Coordinator ^(port 9000^) - Unhealthy
)

curl -s http://localhost:9001/health > nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] DHT Worker 1 ^(port 9001^) - Healthy
) else (
    echo [ERROR] DHT Worker 1 ^(port 9001^) - Unhealthy
)

curl -s http://localhost:9002/health > nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] DHT Worker 2 ^(port 9002^) - Healthy
) else (
    echo [ERROR] DHT Worker 2 ^(port 9002^) - Unhealthy
)

curl -s http://localhost:9003/health > nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] DHT Worker 3 ^(port 9003^) - Healthy
) else (
    echo [ERROR] DHT Worker 3 ^(port 9003^) - Unhealthy
)
goto end

:cleanup
echo Cleaning up DHT services...
docker-compose down -v --remove-orphans
docker system prune -f
echo Cleanup complete!
goto end

:deploy
echo Full DHT deployment starting...
call :build
if %errorlevel% neq 0 exit /b 1
call :start
if %errorlevel% neq 0 exit /b 1
echo Waiting 10 seconds for services to start...
timeout /t 10 /nobreak > nul
call :test
echo Deployment complete!
goto end

:help
echo.
echo WeedHounds DHT Docker Manager
echo.
echo Usage: %0 [command]
echo.
echo Commands:
echo   build       Build DHT services
echo   start       Start DHT services
echo   stop        Stop DHT services
echo   restart     Restart all DHT services
echo   logs        Show logs for all services
echo   logs [name] Show logs for specific service
echo   test        Run DHT system tests
echo   health      Check service health
echo   cleanup     Clean up all containers and volumes
echo   deploy      Full deployment ^(build + start + test^)
echo   help        Show this help message
echo.
echo Examples:
echo   %0 deploy                    Full deployment
echo   %0 logs dht-coordinator      Show coordinator logs
echo   %0 health                    Check all service health
echo.
echo Services:
echo   - DHT Coordinator: http://localhost:9000
echo   - DHT Worker 1:    http://localhost:9001  
echo   - DHT Worker 2:    http://localhost:9002
echo   - DHT Worker 3:    http://localhost:9003
echo.
goto end

:end
