-- dim_songs
SELECT
  artist."ArtistId"
  , artist."Name" AS "ArtistName"
  , album."AlbumId"
  , album."Title"
  , track."TrackId"
  , track."Name"
  , track."UnitPrice"
  , track."Bytes"
  , genre."GenreId" 
  , genre."Name"  AS "GenreName"
  , MAX(DATE(invoice."InvoiceDate")) AS "LastUpdatedAtInvoiceDate"
  , '{{ ts }}' AS "_SnapshotTimestamp"
FROM "Artist" artist
JOIN
  "Album" album ON album."ArtistId" = artist."ArtistId"  
JOIN
  "Track" track ON track."AlbumId" = album."AlbumId"
JOIN 
  "Genre" genre ON genre."GenreId" = track."GenreId"
LEFT JOIN 
  "InvoiceLine" invoice_line ON invoice_line."TrackId" = track."TrackId" 
LEFT JOIN 
  "Invoice" invoice ON invoice."InvoiceId" = invoice_line."InvoiceId"
WHERE date_trunc('month', invoice."InvoiceDate") = '{{ ds }}'
GROUP BY
  artist."ArtistId"
  , artist."Name"
  , album."AlbumId"
  , album."Title"
  , track."TrackId"
  , track."Name"
  , track."UnitPrice"
  , track."Bytes"
  , genre."GenreId"
  , genre."Name"
  , "_SnapshotTimestamp";
