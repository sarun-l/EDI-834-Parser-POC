#!/bin/bash

#process metadata

filename="$1"
#filename="L338_MAGNACARE_834_FULL_5010_20180103_173046.TXT"

sed 's/~/\n/g' "$filename" > metadata.txt

interchange_control_number=$(awk -F "*" '$1=="ISA" {print $14}' metadata.txt)
SponsorName=$(awk -F "*" '$1=="N1" && $2=="P5"  {print $3}' metadata.txt)
PayerName=$(awk -F "*" '$1=="N1" && $2=="IN"  {print $3}' metadata.txt)
ProcessedDate=$(date +'%Y%m%d')

META_ID=$(python3 process_meta.py "$filename" "$interchange_control_number" "$SponsorName" "$PayerName" "$ProcessedDate")
export META_ID


#process ins blocks
sed 's/~/\n/g' "$filename" > test1.txt

ins_start=$(awk -F "*" '$1=="INS" {print NR}' test1.txt | head -n 1)
awk -v ins_start="$ins_start" -F "*" 'NR >= ins_start && $1 != "SE" && $1 != "GE" && $1 != "IEA" {print $0}' test1.txt > test2.txt

awk -F "*" '$1=="INS" {print NR}' test2.txt > ins_pos.txt

declare -a arr

while IFS= read -r line; do
  arr+=("$line")
done < ins_pos.txt

maxindex=(${#arr[@]})
maxindex=$((maxindex - 1))
maxindexvalue=${arr[maxindex]}

linecount=$(wc -l < test2.txt)

index1=0
while [ $index1 -le $maxindex ]
do
	index2=$((index1 + 1))
	
	capture1=${arr[index1]}
	capture2=${arr[index2]}
	capture2=$((capture2 - 1))
	
	if [ "$index1" != "$maxindex" ]; then
		awk -v capture1="$capture1" -v capture2="$capture2" -F "*" 'NR==capture1, NR==capture2 {print $0}' test2.txt > ins_block.txt
	elif [ "$index1" == "$maxindex" ]; then
		awk -v capture1="$capture1" -v linecount="$linecount" -F "*" 'NR==capture1, NR==linecount {print $0}' test2.txt > ins_block.txt
	fi

	index1=$((index1 + 1))
	
	sh process_ins_block.sh
	
done
