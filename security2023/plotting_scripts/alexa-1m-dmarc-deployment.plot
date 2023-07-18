load 'plots/style.gnu'

set datafile separator " "

set lmargin 10

set size 0.6, 0.5
set multiplot
set grid

set key top right
#set yrange [0:100]
set xrange [0:100]

set origin 0, 0.0
set size 0.6, 0.27

set yrange [0:100]
set ytics 20
set key bottom right
set xlabel "{/Helvetica-Bold Alexa Site Rank (bins of 10,000)}"
set ylabel "{/Helvetica-Bold Percentage of domains}" offset 0, 3
set xtics ("0" 0, "200k" 20, "400k" 40, "600k" 60, "800k" 80, "1M" 100)

plot "data/top-1m-result.txt" u ($1):($3/$2*100) w st linestyle 1 lw 3 title "Reporting",\
    "data/top-1m-result.txt" u ($1):($4/$3*100) w st linestyle 8 lw 3 title "Reporting with External Domains"

########### 
set xtics()
set format x ""
set xlabel ""
set ylabel ""
unset label
set origin 0, 0.25
set size 0.6, 0.25

set key top right
set yrange [0:80]
set ytics 20

plot "data/top-1m-result.txt" u ($1):($2/100) w st linestyle 2 lw 3 title "DMARC"

