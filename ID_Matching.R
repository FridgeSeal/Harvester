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
size.original = as.data.frame(dim(id_Data))
size.matched = as.data.frame(dim(matched_pairs))
percent.matched = (size.matched[1]/size.original[1])*100
