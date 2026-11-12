@echo off
setlocal

if "%~1"=="" (
    echo Specify main class
    exit /b 1
)

set "package=it.polimi.middleware.kafka."
set "main_class=%package%%~1"
shift

echo Main class: %main_class%

set "args=%*"

if not "%args%"=="" (
    echo Args: %args%
    mvn exec:java "-Dexec.mainClass=%main_class%" "-Dexec.args=%args%"
) else (
    mvn exec:java "-Dexec.mainClass=%main_class%"
)

endlocal
