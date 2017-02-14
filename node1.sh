ssh c1namenode << EOF
echo "c2namenode">initiator.txt
./slavetask.sh
EOF
exit
