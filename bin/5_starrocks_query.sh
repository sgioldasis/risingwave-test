#!/bin/bash
# StarRocks Query Helper Script
# Provides convenient access to StarRocks MySQL interface

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# MySQL Docker command wrapper
MYSQL_CMD="docker run --rm --network risingwave-test_iceberg_net mysql:8 mysql -hstarrocks-fe -P9030 -uroot"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   StarRocks Query Helper${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Function to execute SQL
execute_sql() {
    local sql="$1"
    $MYSQL_CMD -e "$sql"
}

# Function for interactive shell
interactive_shell() {
    echo -e "${GREEN}Connecting to StarRocks interactive shell...${NC}"
    echo -e "${YELLOW}Note: Type 'exit' or press Ctrl+D to quit${NC}"
    docker run -it --rm --network risingwave-test_iceberg_net mysql:8 \
        mysql -hstarrocks-fe -P9030 -uroot
}

# Menu
show_menu() {
    echo "Select an option:"
    echo "  1) Interactive MySQL shell"
    echo "  2) Show catalogs"
    echo "  3) Show databases"
    echo "  4) Show tables in a database"
    echo "  5) Run custom SQL"
    echo "  6) Query Iceberg catalog"
    echo "  q) Quit"
    echo ""
}

# Main loop
while true; do
    show_menu
    read -p "Enter choice: " choice
    
    case $choice in
        1)
            interactive_shell
            ;;
        2)
            echo -e "${GREEN}Showing configured catalogs...${NC}"
            execute_sql "SHOW CATALOGS;"
            ;;
        3)
            echo -e "${GREEN}Showing databases...${NC}"
            execute_sql "SHOW DATABASES;"
            ;;
        4)
            read -p "Enter database name: " db_name
            if [ -n "$db_name" ]; then
                echo -e "${GREEN}Showing tables in ${db_name}...${NC}"
                execute_sql "SHOW TABLES FROM ${db_name};"
            fi
            ;;
        5)
            read -p "Enter SQL query: " sql
            echo -e "${GREEN}Executing: $sql${NC}"
            execute_sql "$sql"
            ;;
        6)
            echo -e "${GREEN}Querying Iceberg catalog...${NC}"
            read -p "Enter table name (e.g., public.iceberg_countries): " table_name
            if [ -n "$table_name" ]; then
                execute_sql "SELECT * FROM iceberg_catalog.${table_name} LIMIT 10;"
            fi
            ;;
        q|Q)
            echo -e "${GREEN}Goodbye!${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid option${NC}"
            ;;
    esac
    echo ""
    read -p "Press Enter to continue..."
done
