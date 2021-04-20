#!/bin/bash

dialects=("postgres" "mysql" "mssql" "sqlite")

function setup(){
    docker-compose up -d
}

function all(){
    setup;
    for dialect in "${dialects[@]}" ; do
        DEBUG=false GORM_DIALECT=${dialect} go test
    done
}

function postgres(){
    setup;
    DEBUG=false GORM_DIALECT=postgres go test
}

eval "$@"
