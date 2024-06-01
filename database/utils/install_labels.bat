@echo off



echo ============================
echo         IMPORTANT!
echo ============================
echo Things to consider before running this script:
echo 1. Make sure you have the virtual environment created and named '.venv'.
echo 2. Make sure you have installed the required packages listed in 'requirements.pip'.
echo 3. Make sure you have the environment variables set in the '.env' file.
echo 4. Make sure your database is created and running.
echo 5. Make sure you have the apps created in the 'apps' directory. (If you have not created any apps, please create at least one app before running this script.)
echo 6. Make sure you have the models created in the apps.
echo 7. Follow this folder structure for the apps: "apps/<app_name>/models.py".
echo 8. Run this script from directory './database/utils'. To avoid any issues, please do not move this script to another directory.
echo 9. As of now, this script only supports installing labels for all apps. The option to install labels for specific apps will be available in the future.
echo 10. If the labels are already installed, you will see the error messages with the details of the labels that are already installed.

echo.
echo Press any key to continue...
pause > nul


@REM Move to Repository root directory
cd ..
cd ..


@REM Load environment variables
for /f "delims=" %%x in (.env) do set %%x


setlocal enabledelayedexpansion
@REM Apps directory
set APPS_DIR=apps

@REM Apps list
for /d %%i in ("%APPS_DIR%\*") do (
    set APP_NAME=%%~nxi
    set APPS_LIST=!APPS_LIST! apps.!APP_NAME!.models
)
setlocal disabledelayedexpansion



:db_options
echo.
echo.
echo =============================
echo  Create Indexes for Database
echo =============================
echo Choose Database:
echo 1. Production
echo 2. Test

:set_db
echo.
set /p db=Database: 


set URI=%DB_URI%

if /i "%db%"=="Test" (
    set DB_NAME=%TEST_DB_NAME%
    set DB_USER=%TEST_DB_USER%
    set DB_PASS=%TEST_DB_PASS%
) else if /i "%db%"=="Production" (
    set DB_NAME=%DB_NAME%
    set DB_USER=%DB_USER%
    set DB_PASS=%DB_PASS%
) else (
    echo Invalid Database. Please choose between 'Production' and 'Test'
    goto :set_db
)


:apps_options
echo.
echo ===============================
echo  Select apps to install labels
echo ===============================
echo Choose apps to install labels:
echo 1. All apps
echo 2. Specific apps

:set_apps
echo.
set /p apps_install_option=Install option: 


if %apps_install_option%==1 (
    set APPS_LIST_INSTALL=%APPS_LIST%
    goto :configuration_selected

) else if %apps_install_option%==2 (

    echo.
    echo Option not available yet. Please select '1' for now.
    goto :set_apps

    @REM TODO: Implement this feature in the future.
    @REM echo.
    @REM echo Available apps:
    @REM setlocal enabledelayedexpansion
    @REM set count=1
    @REM for %%a in (%APPS_LIST%) do (
    @REM     echo !count!. %%a
    @REM     set app!count!=%%a
    @REM     set /a count+=1
    @REM )
    @REM endlocal

    @REM :select_apps
    @REM echo.
    @REM set /p selected_apps=Select apps to install (by number, separated by comma): 
    @REM setlocal enabledelayedexpansion
    @REM set APPS_LIST_INSTALL=
    @REM set invalid_selection=false
    @REM for %%n in (!selected_apps!) do (
    @REM     set found=false
    @REM     for /l %%c in (1,1,!count!) do (
    @REM         for /f "tokens=1,2 delims==" %%i in ('set app%%c 2^>nul') do (
    @REM             if "%%i"=="app%%c" if "%%n"=="%%c" (
    @REM                 set APPS_LIST_INSTALL=!APPS_LIST_INSTALL! %%j
    @REM                 set found=true
    @REM             )
    @REM         )
    @REM     )
    @REM     if "!found!"=="false" (
    @REM         set invalid_selection=true
    @REM     )
    @REM )
    @REM endlocal

    @REM if "%invalid_selection%"=="false" (
    @REM     goto :configuration_selected
    @REM ) else (
    @REM     echo Invalid selection. Please select valid app numbers.
    @REM     goto :select_apps
    @REM )

) else (
    echo "Invalid option. Please choose between 1 and 2."
    goto :set_apps
)



:configuration_selected
echo.
echo ========================
echo  Configuration Selected
echo ========================
echo Database: %db%
echo User: %DB_USER%
echo URI: %URI%
echo Database Name: %DB_NAME%
echo Apps to install labels:
for %%a in (%APPS_LIST_INSTALL%) do (
    echo . %%a
)

echo.
echo =======================
echo  Confirm Configuration
echo =======================
echo Choose an option:
echo 1. Continue
echo 2. Change Database
echo 3. Change Apps

:confirm
echo.
set /p confirm=Option: 

if %confirm%==1 (
    goto :install_labels
    
) else if %confirm%==2 (
    goto :db_options

) else if %confirm%==3 (
    goto :apps_options

) else (
    echo Invalid option. Please choose between 1, 2 and 3.
    goto :confirm
)



:install_labels
echo.
echo Activating virtual environment...
call .venv\Scripts\activate.bat


echo.
echo Installing labels...
echo.
neomodel_install_labels --db bolt://%DB_USER%:%DB_PASS%@%URI%/%DB_NAME% %APPS_LIST_INSTALL%
echo.
echo Labels installed successfully.

echo.
echo Deactivating virtual environment...
call .venv\Scripts\deactivate.bat

echo.
echo Task completed.

:end
echo.
echo Press any key to exit...
pause > nul