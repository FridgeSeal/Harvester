options(stringsAsFactors = FALSE)
library(data.table) # Because fread is ludicrous fast for big files
library(dplyr)

id_Data = fread(input = 'ID_Pairs.csv', header = TRUE, sep = ',')
target_Pairs = fread(input = 'target_pairs.csv', header = TRUE, sep = ',')
target_Pairs = unique(target_Pairs)
colnames(target_Pairs) = c('ID')
colnames(id_Data) = c('track.IDFA', 'OpID', 'AAID', 'IDFA', 'track.AAID')
id_Data = id_Data[!((id_Data[['AAID']] == 'null') & (id_Data[['IDFA']] == 'null')) & (((id_Data[['IDFA']] != 'null') & ((id_Data[['track.IDFA']] == 'YES') | (id_Data[['track.IDFA']] == 'yes'))) | ((id_Data[['AAID']] != 'null') & ((id_Data[['track.AAID']] == 'null') | (id_Data[['track.AAID']] == 0))))] # It's a single monster line of code
# But in one line it filters out: opID's from Mweb (both idfa and aaid missing)
# idfa's and aaid's with tracking turned off or opted out. :)
# nulls are included because they are the result of some error/missing data and the list they are matched with
# Only has ID's where tracking is enabled, so tracking disabled ID's will get filtered out anyway.
matched_pairs = as.data.frame(sapply(target_Pairs[['ID']], match, id_Data[['OpID']]))
colnames(matched_pairs) = c('row_num')
matched_pairs = id_Data[c(matched_pairs[['row_num']]),]
matched_pairs = unique(matched_pairs[!is.na(matched_pairs[['OpID']]),])
matched_pairs.IDFA = matched_pairs[matched_pairs[['IDFA']] != 'null',]
matched_pairs.IDFA = unique(matched_pairs.IDFA[['IDFA']])
matched_pairs.AAID = matched_pairs[matched_pairs[['AAID']] != 'null',]
matched_pairs.AAID = unique(matched_pairs.AAID[['AAID']])
unmatched_pairs = (anti_join(id_Data, matched_pairs, by = 'OpID'))
unmatched_pairs = unique(unmatched_pairs[['OpID']])
size.original = as.data.frame(dim(id_Data))
size.matched = as.data.frame(dim(matched_pairs))
percent.matched = (size.matched[1]/size.original[1])*100
write.table(matched_pairs.IDFA, file = 'Matched_IDFA.csv', sep = ',', quote = FALSE, row.names = FALSE, col.names = FALSE)
write.table(matched_pairs.AAID, file = 'Matched_AAID.csv', quote = FALSE, sep = ',', row.names = FALSE, col.names = FALSE)
write.table(unmatched_pairs, file = 'unmatched.csv', quote = FALSE, sep = ',', row.names = FALSE, col.names = FALSE)
# TODO turn a lot of this into a function that can be called
# so that we can call it from Python and pass in the csv names to export so we can ultimately have one script that just does everything else