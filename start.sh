input="masters"
while IFS= read -r var
do
  ./INIT.sh "$var" &
done < "$input"
./mastertask.sh
./a.out CombinedData
sleep 10
./regression.out CombinedData
