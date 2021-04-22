SELECT Participants, InstState FROM GRANTS
WHERE Participants LIKE '%[Co Project Director]%'
AND InstState IN ('NY','CA')