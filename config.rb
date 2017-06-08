$settings = {}

$settings[:host]	  = "localhost"
$settings[:basePath]  = "/_csv"

$settings[:dbHost]    = "localhost/db"
$settings[:db]        = "group-national_tablet_program"
$settings[:designDoc] = "ojai" 

# Dev Settings - Uncomment appropriate host and basepath lines for local dev 
$settings[:host]	  = "localhost:9292"
#$settings[:dbHost]	  = "localhost:5984"
$settings[:basePath]  = ""

$settings[:loginUrl] = "http://#{$settings[:dbHost]}/#{$settings[:db]}/_design/#{$settings[:designDoc]}/index.html"
$settings[:login]    = "admin:admin"
$settings[:local]    = false

$settings[:seed] = 0

