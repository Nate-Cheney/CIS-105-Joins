SELECT *
FROM roster INNER JOIN receiving
ON roster.id = receiving.PlayerID;
