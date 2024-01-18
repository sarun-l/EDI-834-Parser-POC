#!/bin/bash

#input: ins_block.txt

#Process INS line
awk -F "*" '$1=="INS" {print $0}' ins_block.txt > ins_line.txt

col0=$(awk -F "*" '{print $1}' ins_line.txt)
col1=$(awk -F "*" '{print $2}' ins_line.txt)
col2=$(awk -F "*" '{print $3}' ins_line.txt)
col3=$(awk -F "*" '{print $4}' ins_line.txt)
col4=$(awk -F "*" '{print $5}' ins_line.txt)
col5=$(awk -F "*" '{print $6}' ins_line.txt)
col6=$(awk -F "*" '{print $7}' ins_line.txt)
col7=$(awk -F "*" '{print $8}' ins_line.txt)
col8=$(awk -F "*" '{print $9}' ins_line.txt)

INS_ID=$(python3 process_ins.py "$col1" "$col2" "$col3" "$col4" "$col5" "$col6" "$col7" "$col8" "$META_ID")


#Process remaining lines in INS block
awk -F "*" '$1!="INS" {print $0}' ins_block.txt > remaining_ins_block.txt

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
		python3 process_remaining_ins.py "$code" "$INS_ID" "$col1" "$col2"
		
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
		python3 process_remaining_ins.py "$code" "$INS_ID" "$col1" "$col2" "$col3" "$col4" "$col5" "$col6" "$col7" "$col8" "$col9" 
		
	elif [ $code = "N3" ]; then
		echo "processing $code line for INS_ID: $INS_ID"
		col1=$(awk -F "*" '{print $2}' remaining_ins_line.txt)
		python3 process_remaining_ins.py "$code" "$INS_ID" "$col1"
		
	elif [ $code = "N4" ]; then
		echo "processing $code line for INS_ID: $INS_ID"
		col1=$(awk -F "*" '{print $2}' remaining_ins_line.txt)
		col2=$(awk -F "*" '{print $3}' remaining_ins_line.txt)
		col3=$(awk -F "*" '{print $4}' remaining_ins_line.txt)
		python3 process_remaining_ins.py "$code" "$INS_ID" "$col1" "$col2" "$col3"
		
	elif [ $code = "DMG" ]; then
		echo "processing $code line for INS_ID: $INS_ID"
		col1=$(awk -F "*" '{print $2}' remaining_ins_line.txt)
		col2=$(awk -F "*" '{print $3}' remaining_ins_line.txt)
		col3=$(awk -F "*" '{print $4}' remaining_ins_line.txt)
		python3 process_remaining_ins.py "$code" "$INS_ID" "$col1" "$col2" "$col3"
		
	elif [ $code = "HD" ]; then
		echo "processing $code line for INS_ID: $INS_ID"
		col1=$(awk -F "*" '{print $2}' remaining_ins_line.txt)
		col2=$(awk -F "*" '{print $3}' remaining_ins_line.txt)
		col3=$(awk -F "*" '{print $4}' remaining_ins_line.txt)
		col4=$(awk -F "*" '{print $5}' remaining_ins_line.txt)
		col5=$(awk -F "*" '{print $6}' remaining_ins_line.txt)
		python3 process_remaining_ins.py "$code" "$INS_ID" "$col1" "$col2" "$col3" "$col4" "$col5"
		
	elif [ $code = "DTP" ]; then
		echo "processing $code line for INS_ID: $INS_ID"
		col1=$(awk -F "*" '{print $2}' remaining_ins_line.txt)
		col2=$(awk -F "*" '{print $3}' remaining_ins_line.txt)
		col3=$(awk -F "*" '{print $4}' remaining_ins_line.txt)
		python3 process_remaining_ins.py "$code" "$INS_ID" "$col1" "$col2" "$col3"

	elif [ $code = "PER" ]; then
		echo "processing $code line for INS_ID: $INS_ID"
		col1=$(awk -F "*" '{print $2}' remaining_ins_line.txt)
		col2=$(awk -F "*" '{print $3}' remaining_ins_line.txt)
		col3=$(awk -F "*" '{print $4}' remaining_ins_line.txt)
		col4=$(awk -F "*" '{print $5}' remaining_ins_line.txt)
		python3 process_remaining_ins.py "$code" "$INS_ID" "$col1" "$col2" "$col3" "$col4"
		
	else
		echo “code not configured, code is $code”
	fi
	
	i=$(( $i + 1 ))
done

