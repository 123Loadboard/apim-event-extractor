@ECHO OFF
SETLOCAL

PUSHD "%~dp0"
SET ROOT=%CD%
POPD

SET JVMARGS=

SET APPARGS=

:NextArg
IF "%~1" == "" GOTO Startup
SET ARG=%~1
IF "%ARG:~0,2%" == "-D" (
	SET JVMARGS=%JVMARGS% %1
) ELSE (
	SET APPARGS=%APPARGS% %1
)
SHIFT
GOTO NextArg

:ProcessArg

:Startup

REM http-correlated/data/year=2020/month=02/day=03/hour=22/min=30/part-00072-9c2580ab-8f81-4629-9af0-e4d7a41151fc.c000.avro

IF DEFINED DEBUG @ECHO ON
java %JVMARGS% %CONFIG_PROPERTIES% -jar "%ROOT%\target\apim-event-extractor-0.0.1-SNAPSHOT.jar" %APPARGS%
@ECHO OFF