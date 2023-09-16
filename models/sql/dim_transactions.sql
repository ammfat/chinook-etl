SELECT 
  track."TrackId"
  , invoice."InvoiceId"
  , customer."CustomerId"
  , DATE(invoice."InvoiceDate") AS "InvoiceDate"
  , invoice."Total"
  , CONCAT(customer."FirstName", ' ', customer."LastName") AS "FullName"
  , customer."Address" 
  , customer."City" 
  , customer."State" 
  , customer."Country" 
  , customer."PostalCode" 
  , customer."Email" 
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
JOIN
  "Customer" customer ON customer."CustomerId" = invoice."CustomerId"
WHERE date_trunc('month', invoice."InvoiceDate") = '{{ ds }}';
