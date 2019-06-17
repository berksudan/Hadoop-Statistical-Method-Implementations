#!/bin/sh

cd $(dirname "$0")
APPDIR="$PWD"

LAUNCHERFILE=$APPDIR/RunHadoopMapRedCalculator.desktop
ICONFILE=$APPDIR/.app_logo.png
LAUNCHERNAME="Run Hadoop MR Calculator"
EXECCOMMAND="java -jar $APPDIR/executables/gui.jar"


printf "#!/usr/bin/env xdg-open
[Desktop Entry]
Version=1.0
Type=Application
Terminal=false
Exec=$EXECCOMMAND
Name[en_US]=$LAUNCHERNAME
Comment[en_US]=$LAUNCHERNAME
Icon[en_US]=$ICONFILE
Name=$LAUNCHERNAME
Comment=$LAUNCHERNAME
Icon=$ICONFILE
Path=$APPDIR
" | tee  $LAUNCHERFILE > /dev/null

chmod +x $LAUNCHERFILE

echo "Icon File: \"$ICONFILE\""
echo "Exec Command of Launcher: \"$EXECCOMMAND\""
echo "Launcher File: \"$LAUNCHERFILE\""
echo "Launcher Name: \"$LAUNCHERNAME\""
