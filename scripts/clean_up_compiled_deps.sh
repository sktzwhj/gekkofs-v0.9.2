#!/bin/bash


DEPS=$(dirname `dirname \`realpath  $0\``)/deps-arm/

find "$DEPS" -print | while read -r file; do
 
   if  [[ -d "$file" ]]; then
        echo "Directory: $file"
	if [ -d "$file/build" ];then
	    rm -rf "$file/build" 
	fi
        if [ $file ==  "$DEPS/syscall_intercept" ];then
	    echo "this is the syscall_intercept directory"
	    rm -rf $file/arch/aarch64/build
	    
	fi
    fi
done
