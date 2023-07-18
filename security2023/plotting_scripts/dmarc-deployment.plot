load 'plots/style.gnu'
set datafile separator ","

set size 0.6, 0.75
set multiplot
set grid
set lmargin 10
set key bottom right
set xdata time
set timefmt '%Y-%m-%d'
set xrange ["2022-01-01": "2023-01-08"]

set origin 0, 0
set size 0.6, 0.3
set yrange [0:100]
set format x "%m/%y" 
set ytics 20

set xlabel ""
set ylabel "{/Helvetica-Bold % of domains}" offset 0, 7
set label "{/Helvetica-Bold DMARC and external reporting}" at "2022-01-11", 10 

plot \
"data/dmarc-deployment-with-tld-mx.csv" u 1:($15/$11*100) w st linestyle 1 lw 3 title '{/Courier-Bold .com}',\
"data/dmarc-deployment-with-tld-mx.csv" u 1:($16/$12*100) w st linestyle 2 lw 3 title '{/Courier-Bold .net}',\
"data/dmarc-deployment-with-tld-mx.csv" u 1:($17/$13*100) w st linestyle 3 lw 3 title '{/Courier-Bold .org}',\
"data/dmarc-deployment-with-tld-mx.csv" u 1:($18/$14*100) w st linestyle 4 lw 3 title '{/Courier-Bold .se}',\

###############
set xtics()
unset label
set size 0.6, 0.25
set origin 0, 0.28

set format x ""
set yrange [0:80]
set ytics 20
set xlabel ""
set ylabel ""
#set ylabel "{/Helvetica-Bold % of DMARC domains}\n{/Helvetica-Bold w/ reporting}"
set label "{/Helvetica-Bold DMARC and reporting}" at "2022-01-11", 10

plot \
"data/dmarc-deployment-with-tld-mx.csv" u 1:($11/$7*100) w st linestyle 1 lw 3 title '',\
"data/dmarc-deployment-with-tld-mx.csv" u 1:($12/$8*100) w st linestyle 2 lw 3 title '',\
"data/dmarc-deployment-with-tld-mx.csv" u 1:($13/$9*100) w st linestyle 3 lw 3 title '',\
"data/dmarc-deployment-with-tld-mx.csv" u 1:($14/$10*100) w st linestyle 4 lw 3 title '',\

###############
set xtics()
unset label
set size 0.6, 0.25
set origin 0, 0.28 + 0.23

set xlabel ""
set ylabel ""
set format x ""
set yrange [0:10]
set ytics 2
#set ylabel "{/Helvetica-Bold % of domains}\n{/Helvetica-Bold w/ DMARC}"
set label "{/Helvetica-Bold DMARC}" at "2022-01-11", 1

plot \
"data/dmarc-deployment-with-tld-mx.csv" u 1:($7/$19*100) w st linestyle 1 lw 3 title '',\
"data/dmarc-deployment-with-tld-mx.csv" u 1:($8/$20*100) w st linestyle 2 lw 3 title '',\
"data/dmarc-deployment-with-tld-mx.csv" u 1:($9/$21*100) w st linestyle 3 lw 3 title '',\
"data/dmarc-deployment-with-tld-mx.csv" u 1:($10/$22*100) w st linestyle 4 lw 3 title ''\
