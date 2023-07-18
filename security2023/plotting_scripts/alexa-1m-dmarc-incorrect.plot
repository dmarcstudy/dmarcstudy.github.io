load 'plots/style.gnu'
set datafile separator " "

set size 0.6, 0.3
set grid

set key top right
set yrange [0:30]
set key bottom right
set xlabel "{/Helvetica-Bold Alexa Site Rank (bins of 10,000)}"
set ylabel "{/Helvetica-Bold \% of domains}\n{/Helvetica-Bold w/ ext. reporting}" 
set xtics ("0" 0, "200k" 20, "400k" 40, "600k" 60, "800k" 80, "1M" 100)

plot \
"data/top-1m-result.txt" u ($1):($5/$4 * 100) w st linestyle 1 lw 3 title "w/o DMARC authorization record"

