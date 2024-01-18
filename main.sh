#!/bin/bash

set -e

filename="$1"


process_metadata() 
	{
	file="$1"
	
	sed 's/~/\n/g' "$file" > metadata.txt

	interchange_control_number=$(awk -F "*" '$1=="ISA" {print $14}' metadata.txt)
	SponsorName=$(awk -F "*" '$1=="N1" && $2=="P5"  {print $3}' metadata.txt)
	PayerName=$(awk -F "*" '$1=="N1" && $2=="IN"  {print $3}' metadata.txt)
	ProcessedDate=$(date +'%Y%m%d')
		
	META_ID=$(python3 -c "from db_insert import process_metadata; print(process_metadata('$file', '$interchange_control_number', '$SponsorName', '$PayerName', '$ProcessedDate'))")
	export META_ID
	}


process_ins_block() 
	{
	#input: ins_block.txt
	ins_block="$1"
	
	#Process INS line in INS block
	awk -F "*" '$1=="INS" {print $0}' "$ins_block" > ins_line.txt
	
	col0=$(awk -F "*" '{print $1}' ins_line.txt)
	col1=$(awk -F "*" '{print $2}' ins_line.txt)
	col2=$(awk -F "*" '{print $3}' ins_line.txt)
	col3=$(awk -F "*" '{print $4}' ins_line.txt)
	col4=$(awk -F "*" '{print $5}' ins_line.txt)
	col5=$(awk -F "*" '{print $6}' ins_line.txt)
	col6=$(awk -F "*" '{print $7}' ins_line.txt)
	col7=$(awk -F "*" '{print $8}' ins_line.txt)
	col8=$(awk -F "*" '{print $9}' ins_line.txt)
	
	INS_ID=$(python3 -c "from db_insert import process_ins_line; print(process_ins_line('$col1','$col2','$col3','$col4','$col5','$col6','$col7','$col8','$META_ID'))")
	
	
	#Process remaining lines in INS block
	awk -F "*" '$1!="INS" {print $0}' "$ins_block" > remaining_ins_block.txt
	
	linecount=$(wc -l < remaining_ins_block.txt)
	i=1

	while [ $i -le $linecount ]
	do
		awk -v i="$i" -F "*" 'NR==i {print $0}' remaining_ins_block.txt > remaining_ins_line.txt
		cat remaining_ins_line.txt	
		
		code=$(awk -F "*" '{print $1}' remaining_ins_line.txt)
		
		if [ $code = "REF" ]; then
			echo "processing $code line for INS_ID: $INS_ID"
			col1=$(awk -F "*" '{print $2}' remaining_ins_line.txt)
			col2=$(awk -F "*" '{print $3}' remaining_ins_line.txt)
			python3 -c "from db_insert import process_remaining_ins; process_remaining_ins('$code', '$INS_ID', '$col1', '$col2')"
			
		elif [ $code = "NM1" ]; then
			echo "processing $code line for INS_ID: $INS_ID"
			col1=$(awk -F "*" '{print $2}' remaining_ins_line.txt)
			col2=$(awk -F "*" '{print $3}' remaining_ins_line.txt)
			col3=$(awk -F "*" '{print $4}' remaining_ins_line.txt)
			col4=$(awk -F "*" '{print $5}' remaining_ins_line.txt)
			col5=$(awk -F "*" '{print $6}' remaining_ins_line.txt)
			col6=$(awk -F "*" '{print $7}' remaining_ins_line.txt)
			col7=$(awk -F "*" '{print $8}' remaining_ins_line.txt)
			col8=$(awk -F "*" '{print $9}' remaining_ins_line.txt)
			col9=$(awk -F "*" '{print $10}' remaining_ins_line.txt)
			python3 -c "from db_insert import process_remaining_ins; process_remaining_ins('$code', '$INS_ID', '$col1', '$col2', '$col3', '$col4', '$col5', '$col6', '$col7', '$col8', '$col9')"
			
		elif [ $code = "N3" ]; then
			echo "processing $code line for INS_ID: $INS_ID"
			col1=$(awk -F "*" '{print $2}' remaining_ins_line.txt)
			python3 -c "from db_insert import process_remaining_ins; process_remaining_ins('$code', '$INS_ID', '$col1')"
			
		elif [ $code = "N4" ]; then
			echo "processing $code line for INS_ID: $INS_ID"
			col1=$(awk -F "*" '{print $2}' remaining_ins_line.txt)
			col2=$(awk -F "*" '{print $3}' remaining_ins_line.txt)
			col3=$(awk -F "*" '{print $4}' remaining_ins_line.txt)
			python3 -c "from db_insert import process_remaining_ins; process_remaining_ins('$code', '$INS_ID', '$col1', '$col2', '$col3')"
			
		elif [ $code = "DMG" ]; then
			echo "processing $code line for INS_ID: $INS_ID"
			col1=$(awk -F "*" '{print $2}' remaining_ins_line.txt)
			col2=$(awk -F "*" '{print $3}' remaining_ins_line.txt)
			col3=$(awk -F "*" '{print $4}' remaining_ins_line.txt)
			python3 -c "from db_insert import process_remaining_ins; process_remaining_ins('$code', '$INS_ID', '$col1', '$col2', '$col3')"
			
		elif [ $code = "HD" ]; then
			echo "processing $code line for INS_ID: $INS_ID"
			col1=$(awk -F "*" '{print $2}' remaining_ins_line.txt)
			col2=$(awk -F "*" '{print $3}' remaining_ins_line.txt)
			col3=$(awk -F "*" '{print $4}' remaining_ins_line.txt)
			col4=$(awk -F "*" '{print $5}' remaining_ins_line.txt)
			col5=$(awk -F "*" '{print $6}' remaining_ins_line.txt)
			python3 -c "from db_insert import process_remaining_ins; process_remaining_ins('$code','$INS_ID','$col1','$col2','$col3','$col4','$col5')"
			
		elif [ $code = "DTP" ]; then
			echo "processing $code line for INS_ID: $INS_ID"
			col1=$(awk -F "*" '{print $2}' remaining_ins_line.txt)
			col2=$(awk -F "*" '{print $3}' remaining_ins_line.txt)
			col3=$(awk -F "*" '{print $4}' remaining_ins_line.txt)
			python3 -c "from db_insert import process_remaining_ins; process_remaining_ins('$code','$INS_ID','$col1','$col2','$col3')"

		elif [ $code = "PER" ]; then
			echo "processing $code line for INS_ID: $INS_ID"
			col1=$(awk -F "*" '{print $2}' remaining_ins_line.txt)
			col2=$(awk -F "*" '{print $3}' remaining_ins_line.txt)
			col3=$(awk -F "*" '{print $4}' remaining_ins_line.txt)
			col4=$(awk -F "*" '{print $5}' remaining_ins_line.txt)
			python3 -c "from db_insert import process_remaining_ins; process_remaining_ins('$code', '$INS_ID', '$col1', '$col2', '$col3', '$col4')"
			
		else
			echo "code $code not configured"
		fi
		
		i=$(( $i + 1 ))
	done
	}


process_ins_main() 
	{
	file="$1"
	sed 's/~/\n/g' "$file" > initial_block.txt
	
	ins_start=$(awk -F "*" '$1=="INS" {print NR}' initial_block.txt | head -n 1)
	echo "$file"
	
	awk -v ins_start="$ins_start" -F "*" 'NR >= ins_start && $1 != "SE" && $1 != "GE" && $1 != "IEA" {print $0}' initial_block.txt > working_block.txt
	awk -F "*" '$1=="INS" {print NR}' working_block.txt > ins_position.txt
	
	
	declare -a arr
	
	while IFS= read -r line; do
	  arr+=("$line")
	done < ins_position.txt
	
	maxindex=(${#arr[@]})
	maxindex=$((maxindex - 1))
	
	linecount=$(wc -l < working_block.txt)
	index1=0
	
	while [ $index1 -le $maxindex ];
	do
		index2=$((index1 + 1))
		
		capture1=${arr[index1]}
		capture2=${arr[index2]}
		capture2=$((capture2 - 1))
		
		if [ "$index1" != "$maxindex" ]; then
			awk -v capture1="$capture1" -v capture2="$capture2" -F "*" 'NR==capture1, NR==capture2 {print $0}' working_block.txt > ins_block.txt
		elif [ "$index1" == "$maxindex" ]; then
			awk -v capture1="$capture1" -v linecount="$linecount" -F "*" 'NR==capture1, NR==linecount {print $0}' working_block.txt > ins_block.txt
		fi

		#call function
		process_ins_block ins_block.txt
		
		index1=$((index1 + 1))
		
	done
	}

	
process_metadata "$filename"
process_ins_main "$filename"
