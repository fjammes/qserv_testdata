-- interesting syntax

SELECT sce.filterId, sce.filterName
FROM   Science_Ccd_Exposure AS sce
WHERE  (sce.visit = 887404831)
   AND (sce.raftName LIKE "'2%")
   AND (sce.ccdName LIKE '%')
