#!/bin/bash

if [[ $# -eq 0 ]]; then
    echo "Specify main class"
    exit 1
fi

package="it.polimi.middleware.kafka."
main_class="$package$1"
shift
echo "Main class: $main_class"

args="$*"

mvn_args=("-Dexec.mainClass=$main_class")

if [[ -n "$args" ]]; then
    mvn_args+=("-Dexec.args=$args")
    echo "Args: $args"
fi

mvn exec:java "${mvn_args[@]}"
