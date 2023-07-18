load 'plots/style.gnu'
set datafile separator " "

set size 0.6, 0.4
set origin 0, 0
set grid
set colorsequence default

set logscale x 
# set xrange [0:]
#set xtics ("0" 0, "50k" 50000, "100k" 100000, "150k" 150000, "200k" 200000, "250k" 250000, "300k" 300000, "350k" 350000, "400k" 400000, "500k" 500000)
set ytics 0.1
#set yrange [0:300000]
# set logscale y 10
set yrange [0:1]

set xlabel "{/Helvetica-Bold Domain of Report Receiver}"
set ylabel "{/Helvetica-Bold CDF}"

# plot "data/count-report-centrality-with-mx-sorted.txt" u ($1+1):2 w st linestyle 1 lw 4 notitle
plot "data/cdf-report-centrality.txt" u 1:2 w st linestyle 1 lw 4 notitle
