SELECT 
  artist."ArtistId"
  , artist."Name"
  , album."Title"
  , track."Name" AS "TrackName"
  , invoice_line."UnitPrice" * invoice_line."Quantity" AS "TotalPrice"
  , invoice_line."InvoiceLineId"
  , invoice."InvoiceId"
  , DATE(invoice."InvoiceDate") AS "InvoiceDate"
  , '{{ ts }}' AS "_SnapshotTimestamp"
FROM "Artist" artist
JOIN
  "Album" album ON album."ArtistId" = artist."ArtistId"  
JOIN
  "Track" track ON track."AlbumId" = album."AlbumId"
JOIN 
  "InvoiceLine" invoice_line ON invoice_line."TrackId" = track."TrackId" 
JOIN 
  "Invoice" invoice ON invoice."InvoiceId" = invoice_line."InvoiceId"
WHERE date_trunc('month', invoice."InvoiceDate") = '{{ ds }}';
