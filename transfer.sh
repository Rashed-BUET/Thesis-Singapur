initiator=$(head -n 1 initiator.txt)
cd RegressionOutput/
echo $initiator
for file in *
do
        x=${file##*/}
        echo $x
	if [[ -d $x ]]; then
        cd "$x"/
        for y in *
        do
                if [[ "${y##*/}" = "part-r-00000" ]]; then
                        echo "found"
                        mv "${y##*/}" "c2""$x"
                        scp "c2""$x" "$initiator":"RegressionOutput/""$x"
                fi;
        done
        cd ..
        fi
done

