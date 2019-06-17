#!/bin/sh

cd $(dirname "$0")
APPDIR="$PWD"

LAUNCHERFILE=$APPDIR/RunHadoopMapRedCalculator.desktop
ICONFILE=$APPDIR/.app_logo.png
APPNAME="Run Hadoop MR Calculator"

EXECCOMMAND="java -jar $APPDIR/executables/gui.jar"

printf "Let's create a desktop launcher of your app:\n"
printf "What is your app's command?\n"

touch ~/Desktop/$appropriateName.desktop

echo "Created: ~/Desktop/$appropriateName"

printf "#!/usr/bin/env xdg-open
[Desktop Entry]
Version=1.0
Type=Application
Terminal=false
Exec=$EXECCOMMAND
Name[en_US]=$APPNAME
Comment[en_US]=$APPNAME
Icon[en_US]=$ICONFILE
Name=$APPNAME
Comment=$APPNAME
Icon=$ICONFILE
" | tee  $LAUNCHERFILE

chmod +x $LAUNCHERFILE

printf "\nYour app launcher is ready: <<$LAUNCHERFILE>>" 
