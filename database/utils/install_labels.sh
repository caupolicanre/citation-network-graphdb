#!/bin/bash

echo "============================"
echo "        IMPORTANT!"
echo "============================"
echo "Things to consider before running this script:"
echo "1. Make sure you have the virtual environment created and named '.venv'."
echo "2. Make sure you have installed the required packages listed in 'requirements.pip'."
echo "3. Make sure you have the environment variables set in the '.env' file."
echo "4. Make sure your database is created and running."
echo "5. Make sure you have the apps created in the 'apps' directory. (If you have not created any apps, please create at least one app before running this script.)"
echo "6. Make sure you have the models created in the apps."
echo "7. Follow this folder structure for the apps: 'apps/<app_name>/models.py'."
echo "8. Run this script from directory './database/utils'. To avoid any issues, please do not move this script to another directory."
echo "9. As of now, this script only supports installing labels for all apps. The option to install labels for specific apps will be available in the future."
echo "10. If the labels are already installed, you will see the error messages with the details of the labels that are already installed."

echo
echo "Press any key to continue..."
read -n 1 -s

# Move to Repository root directory
cd ../..

# Load environment variables
export $(grep -v '^#' .env | xargs)

# Apps directory
APPS_DIR="apps"

# Apps list
APPS_LIST=""
for dir in $APPS_DIR/*/ ; do
    APP_NAME=$(basename "$dir")
    APPS_LIST="$APPS_LIST apps.$APP_NAME.models"
done

db_options() {
    echo
    echo
    echo "============================="
    echo " Create Indexes for Database"
    echo "============================="
    echo "Choose Database:"
    echo "1. Production"
    echo "2. Test"

    set_db
}

set_db() {
    echo
    read -p "Database: " db

    URI=$DB_URI

    if [ "$db" == "Test" ] || [ "$db" == "2" ]; then
        DB_NAME=$TEST_DB_NAME
        DB_USER=$TEST_DB_USER
        DB_PASS=$TEST_DB_PASS
    elif [ "$db" == "Production" ] || [ "$db" == "1" ]; then
        DB_NAME=$DB_NAME
        DB_USER=$DB_USER
        DB_PASS=$DB_PASS
    else
        echo "Invalid Database. Please choose between 'Production' and 'Test'"
        set_db
    fi
}

apps_options() {
    echo
    echo "=============================="
    echo " Select apps to install labels"
    echo "=============================="
    echo "Choose apps to install labels:"
    echo "1. All apps"
    echo "2. Specific apps"

    set_apps
}

set_apps() {
    echo
    read -p "Install option: " apps_install_option

    if [ "$apps_install_option" == "1" ]; then
        APPS_LIST_INSTALL=$APPS_LIST
        configuration_selected
    elif [ "$apps_install_option" == "2" ]; then
        echo
        echo "Option not available yet. Please select '1' for now."
        set_apps
    else
        echo "Invalid option. Please choose between 1 and 2."
        set_apps
    fi
}

configuration_selected() {
    echo
    echo "========================"
    echo " Configuration Selected"
    echo "========================"
    echo "Database: $db"
    echo "User: $DB_USER"
    echo "URI: $URI"
    echo "Database Name: $DB_NAME"
    echo "Apps to install labels:"
    for app in $APPS_LIST_INSTALL; do
        echo ". $app"
    done

    echo
    echo "======================="
    echo " Confirm Configuration"
    echo "======================="
    echo "Choose an option:"
    echo "1. Continue"
    echo "2. Change Database"
    echo "3. Change Apps"

    confirm
}

confirm() {
    echo
    read -p "Option: " confirm

    if [ "$confirm" == "1" ]; then
        install_labels
    elif [ "$confirm" == "2" ]; then
        db_options
    elif [ "$confirm" == "3" ]; then
        apps_options
    else
        echo "Invalid option. Please choose between 1, 2 and 3."
        confirm
    fi
}

install_labels() {
    echo
    echo "Activating virtual environment..."
    source .venv/bin/activate

    echo
    echo "Installing labels..."
    echo
    neomodel_install_labels --db "bolt://$DB_USER:$DB_PASS@$URI/$DB_NAME" $APPS_LIST_INSTALL
    echo
    echo "Labels installed successfully."

    echo
    echo "Deactivating virtual environment..."
    deactivate

    echo
    echo "Task completed."
}

# Start script
db_options
apps_options
configuration_selected

echo
echo "Press any key to exit..."
read -n 1 -s
