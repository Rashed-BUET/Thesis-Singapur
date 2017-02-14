ssh "$1" << EOF
echo "c2namenode">initiator.txt
./slavetask.sh
EOF
exit
