class Brockman < Sinatra::Base
  
  get '/email/:email/:group/:workflowIds/:year/:month/:county.?:format?' do | email, group, workflowIds, year, month, county, format |

    return if format.match(/png/)

    requestId = SecureRandom.base64

    $logger.info "email - #{group}"

    couch = Couch.new({
      :host      => $settings[:host],
      :login     => $settings[:login],
      :designDoc => $settings[:designDoc],
      :db        => group
    })

    # @hardcode who is formal
    formalZones = ["waruku","posta","silanga","kayole","gichagi","congo","zimmerman","chokaa"]
    subjectLegend = { "english_word" => "English", "word" => "Kiswahili", "operation" => "Maths" }

    geography = couch.getRequest( { :document => "geography-quotas" } )
    quotasByZones  = {}
    quotasByCounty = {}
    quotaNational = 0

    geography['counties'].map { | countyName, county |
      countyName = countyTranslate(countyName.downcase)
      quotasByCounty[countyName] = county['quota']
      county['zones'].map { | zone, quota |
        zone = zoneTranslate(zone)
        quotasByZones[zone.downcase] = quota
        quotaNational += quota.to_i
      }
    }

    byZone = {}

    # get trips from month specified
    monthKey        = "\"year#{year}month#{month}\""
    tripsFromMonth  = couch.getRequest({ :view => "tutorTrips", :params => { :key => monthKey } } )

    tripIdsFromMonth = tripsFromMonth['rows'].map{ |e| e['value'] }

    # if workflows specified, filter trips to those workflows
    if workflowIds != "all"
      workflowKeys      = workflowIds.split(",").map{ |s| "workflow-#{s}" }
      tripsFromWorkflow = couch.postRequest({ :view => "tutorTrips", :data => { "keys" => workflowKeys } } )['rows'].map{ |e| e['value'] }
      tripIds           = tripIdsFromMonth & tripsFromWorkflow # intersection
    end

    # get summaries from trips
    tripKeys      = tripIds.uniq

    tripsResponse = couch.postRequest({ :view => "spirtRotut?group=true", :data => { "keys" => tripKeys } } )

    tripRows = tripsResponse['rows']

    #
    # filter rows
    #

    tripRows = tripRows.select { | row |
      longEnough = ( row['value']['maxTime'].to_i - row['value']['minTime'].to_i ) / 1000 / 60 >= 20
      longEnough
    }

    # used for keys for this request
    monthGroup = "#{group}#{year}#{month}"

    result = {}

    result['visits'] = CacheHandler::tryCache "email-visits-#{monthGroup}-#{tripKeys.join}", lambda {
      byZone = {}
      byCounty = {}
      national = 0

      for sum in tripRows
        next if sum['value']['zone'].nil?
        zoneName   = zoneTranslate(sum['value']['zone'].downcase)
        countyName = countyTranslate(sum['value']['county'].downcase)

        byZone[zoneName] = 0 unless byZone[zoneName]
        byZone[zoneName] += 1

        byCounty[countyName] = 0 unless byCounty[countyName]
        byCounty[countyName] += 1

        national += 1 

      end
      return {
        "byZone" => byZone,
        "byCounty" => byCounty,
        "national" => national
      }
    }


    result['fluency'] = CacheHandler::tryCache "email-fluency-#{monthGroup}-#{tripKeys.join}", lambda {

      byZone = {}
      byCounty = {}
      national = {}
      subjects = []

      for sum in tripRows

        next if sum['value']['zone'].nil? or sum['value']['itemsPerMinute'].nil?
        next if sum['value']['subject'].nil? or sum['value']['subject'] == "" 

        zoneName   = zoneTranslate(sum['value']['zone'].downcase)
        countyName = countyTranslate(sum['value']['county'].downcase)
        itemsPerMinute = sum['value']['itemsPerMinute']
        benchmarked    = sum['value']['benchmarked']

        subject = sum['value']['subject']

        subjects.push subject if not subjects.include? subject

        total = 0
        itemsPerMinute.each { | ipm | total += ipm }

        byZone[zoneName]          = {}        unless byZone[zoneName]
        byZone[zoneName][subject] = {}        unless byZone[zoneName][subject]
        byZone[zoneName][subject]['sum']  = 0 unless byZone[zoneName][subject]['sum']
        byZone[zoneName][subject]['size'] = 0 unless byZone[zoneName][subject]['size']

        byZone[zoneName][subject]['sum']  += total
        byZone[zoneName][subject]['size'] += benchmarked

        byCounty[countyName]                  = {} unless byCounty[countyName]
        byCounty[countyName][subject]         = {} unless byCounty[countyName][subject]
        byCounty[countyName][subject]['sum']  = 0  unless byCounty[countyName][subject]['sum']
        byCounty[countyName][subject]['size'] = 0  unless byCounty[countyName][subject]['size']

        byCounty[countyName][subject]['sum']  += total
        byCounty[countyName][subject]['size'] += benchmarked

        national                  = {} unless national
        national[subject]         = {} unless national[subject]
        national[subject]['sum']  = 0  unless national[subject]['sum']
        national[subject]['size'] = 0  unless national[subject]['size']

        national[subject]['sum']  += total
        national[subject]['size'] += benchmarked

      end

      subjects = subjects.select  { |x| subjectLegend.keys.include? x }
      subjects = subjects.sort_by { |x| subjectLegend.keys.index(x) }

      return {
        "byZone" => byZone,
        "byCounty" => byCounty,
        "national" => national,
        "subjects" => subjects
      }
    } # result['fluency']

    result['metBenchmark'] = CacheHandler::tryCache "email-met-benchmark-#{monthGroup}-#{tripKeys.join}", lambda {

      byZone   = {}
      byCounty = {}
      national = {}

      for sum in tripRows

        next if sum['value']['zone'].nil? or sum['value']['subject'].nil?

        zoneName   = zoneTranslate(sum['value']['zone'].downcase)
        countyName = countyTranslate(sum['value']['county'].downcase)
        subject    = sum['value']['subject'].downcase

        met = sum['value']['metBenchmark']

        byZone[zoneName] = {} unless byZone[zoneName]
        byZone[zoneName][subject] = 0 unless byZone[zoneName][subject]
        byZone[zoneName][subject] += met

        byCounty[countyName] = {} unless byCounty[countyName]
        byCounty[countyName][subject] = 0 unless byCounty[countyName][subject]
        byCounty[countyName][subject] += met

        national[subject] = 0 unless national[subject]
        national[subject] += met

      end

      return {
        "byZone" => byZone,
        "byCounty" => byCounty,
        "national" => national
      }

    } # result['metBenchmark']



    result['zonesByCounty'] = CacheHandler::tryCache "email-zones-by-county-#{monthGroup}-#{tripKeys.join}", lambda {
      counties = {}
      for sum in tripRows

        next if sum['value']['zone'].nil?

        zoneName   = zoneTranslate(sum['value']['zone'].downcase)
        countyName = countyTranslate(sum['value']['county'].downcase)

        counties[countyName] = [] unless counties[countyName]
        counties[countyName].push(zoneName) unless counties[countyName].include?(zoneName)
      end
      return counties
    }

    if ! result['zonesByCounty'][county.downcase].nil?
      zones = result['zonesByCounty'][county.downcase].sort_by{|word| word.downcase}
    else
      zones = []
    end




    legendHtml = "
      <small>
        <ol>
          <li id='footer-note-1'><b>Number of classroom visits</b> are defined as Full PRIMR or Best Practices classroom observations that include all forms and all 3 assessments, with at least 20 minutes duration, and took place between 7AM and 2PM of any calendar day during the selected month.</li>
          <li id='footer-note-2'><b>Targeted number of classroom visits</b> is equivalent to the number of class 1 and class 2 teachers in each zone.</li>
          <li id='footer-note-3'><b>Correct per minute</b> is the calculated average out of all individual assessment results from all qualifying classroom visits in the selected month to date, divided by the total number of assessments conducted.</li>
          <li id='footer-note-4'><b>Percentage at KNEC benchmark</b> is the percentage of those students that have met the KNEC benchmark for either Kiswahili or English, and for either class 1 or class 2, out of all of the students assessed for those subjects.</li>
        </ol>
        <ul style='list-style:none;'>
          <li><b>*</b> Non-formal</li>
        </ul>
      </small>
    "

    

    row = 0
    zoneTableHtml = "
      <table class='dataTable'>
        <thead>
          <tr>
            <th class='sorting'>Zone</th>
            <th class='sorting'>Number of classroom visits <a href='#footer-note-1'><sup>[1]</sup></a></th>
            <th class='sorting'>Targeted number of classroom visits<a href='#footer-note-2'><sup>[2]</sup></a></th>
            #{result['fluency']['subjects'].select{|x|x!="3" && !x.nil?}.map{ | subject |
              "<th class='sorting'>#{subjectLegend[subject]}<br>Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>#{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>" if subject != "operation"}</th>"
            }.join}
          </tr>
        </thead>
        <tbody>
          #{zones.map{ |zone|

            row += 1

            zone = zone.downcase

            next if result['fluency']['byZone'][zone].nil?

            visits = result['visits']['byZone'][zone]

            met = result['metBenchmark']['byZone'][zone]

            quota = quotasByZones[zone]

            sampleTotal = 0

            nonFormalAsterisk = if formalZones.include? zone.downcase then "<b>*</b>" else "" end

          "
            <tr class='#{if row % 2 == 0 then "even" else "odd" end }'> 
              <td>#{zone.capitalize} #{nonFormalAsterisk}</td>
              <td>#{visits}</td>
              <td>#{quota}</td>
              #{result['fluency']['subjects'].select{|x|x!="3" && !x.nil?}.map{ | subject |
                sample = result['fluency']['byZone'][zone][subject]
                if sample.nil?
                  average = "no data"
                else
                  
                  if sample && sample['size'] != 0 && sample['sum'] != 0
                    sampleTotal += sample['size']
                    average = ( sample['sum'] / sample['size'] ).round
                  else
                    average = '0'
                  end

                  if subject != 'operation'
                    benchmark = result['metBenchmark']['byZone'][zone][subject]
                    percentage = percentage( sample['size'], benchmark )
                    benchmarks = "( #{percentage}% )"
                  end

                end

                "<td>#{average} #{benchmarks}</td>"
              }.join}

            </tr>
          "}.join }
        </tbody>
      </table>
      #{legendHtml}
    "

    zoneHtml = "
      <h2>
        #{county.capitalize} County Report
        #{year} #{["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month.to_i]}
      </h2>
      #{zoneTableHtml}
    "

    row = 0
    countiesTableHtml = "
      <table class='dataTable'>
        <thead>
          <tr>
            <th class='sorting'>County</th>
            <th class='sorting'>Number of classroom visits<a href='#footer-note-1'><sup>[1]</sup></a></th>
            <th class='sorting'>Targeted number of classroom visits<a href='#footer-note-2'><sup>[2]</sup></a></th>
            #{result['fluency']['subjects'].map{ | subject |
              "<th class='sorting'>#{subjectLegend[subject]}<br>
                Correct per minute<a href='#footer-note-3'><sup>[3]</sup></a><br>
                #{"<small>( Percentage at KNEC benchmark<a href='#footer-note-4'><sup>[4]</sup></a>)</small>" if subject != "operation"}
              </th>"
            }.join}
          </tr>
        </thead>
        <tbody>
          #{ result['visits']['byCounty'].map{ | county, visits |

            row += 1

            county = county.downcase

            met = result['metBenchmark']['byCounty'][county]

            quota = quotasByCounty[county]

            sampleTotal = 0

          "
            <tr class='#{if row % 2 == 0 then "even" else "odd" end }'>
              <td>#{county.capitalize}</td>
              <td>#{visits}</td>
              <td>#{quota}</td>
              #{result['fluency']['subjects'].map{ | subject |
                sample = result['fluency']['byCounty'][county][subject]
                if sample.nil?
                  average = "no data"
                else
                  if sample && sample['size'] != 0 && sample['sum'] != 0
                    sampleTotal += sample['size']
                    average = ( sample['sum'] / sample['size'] ).round
                  else
                    average = '0'
                  end

                  if subject != "operation"
                    benchmark = result['metBenchmark']['byCounty'][county][subject]
                    percentage = "( #{percentage( sample['size'], benchmark )}% )"
                  end
                end
                "<td>#{average} #{percentage}</td>"
              }.join}
            </tr>
          "}.join }
        </tbody>
      </table>
      #{legendHtml}

    "

    countiesHtml = "
      <h2>
        #{county.capitalize} County Report
        #{year} #{["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month.to_i]}
      </h2>
      #{countiesTableHtml}
    "

    if county.downcase != "all"
      contentHtml = zoneHtml
    else
      contentHtml = countiesHtml
    end

    html =  "
      <html>
        <head>
          <style>
            body{font-family:Helvetica;}
            table.dataTable{margin:0 auto;clear:both;width:100%}table.dataTable thead th{padding:3px 18px 3px 10px;border-bottom:1px solid #000;font-weight:700;cursor:pointer;*cursor:hand}table.dataTable tfoot th{padding:3px 18px 3px 10px;border-top:1px solid #000;font-weight:700}table.dataTable td{padding:3px 10px}table.dataTable td.center,table.dataTable td.dataTables_empty{text-align:center}table.dataTable tr.odd{background-color:#E2E4FF}table.dataTable tr.even{background-color:#fff}table.dataTable tr.odd td.sorting_1{background-color:#D3D6FF}table.dataTable tr.odd td.sorting_2{background-color:#DADCFF}table.dataTable tr.odd td.sorting_3{background-color:#E0E2FF}table.dataTable tr.even td.sorting_1{background-color:#EAEBFF}table.dataTable tr.even td.sorting_2{background-color:#F2F3FF}table.dataTable tr.even td.sorting_3{background-color:#F9F9FF}.dataTables_wrapper{position:relative;clear:both;*zoom:1}.dataTables_length{float:left}.dataTables_filter{float:right;text-align:right}.dataTables_info{clear:both;float:left}.dataTables_paginate{float:right;text-align:right}.paginate_disabled_next,.paginate_disabled_previous,.paginate_enabled_next,.paginate_enabled_previous{height:19px;float:left;cursor:pointer;*cursor:hand;color:#111!important}.paginate_disabled_next:hover,.paginate_disabled_previous:hover,.paginate_enabled_next:hover,.paginate_enabled_previous:hover{text-decoration:none!important}.paginate_disabled_next:active,.paginate_disabled_previous:active,.paginate_enabled_next:active,.paginate_enabled_previous:active{outline:0}.paginate_disabled_next,.paginate_disabled_previous{color:#666!important}.paginate_disabled_previous,.paginate_enabled_previous{padding-left:23px}.paginate_disabled_next,.paginate_enabled_next{padding-right:23px;margin-left:10px}.paginate_enabled_previous{background:url(../images/back_enabled.png) no-repeat top left}.paginate_enabled_previous:hover{background:url(../images/back_enabled_hover.png) no-repeat top left}.paginate_disabled_previous{background:url(../images/back_disabled.png) no-repeat top left}.paginate_enabled_next{background:url(../images/forward_enabled.png) no-repeat top right}.paginate_enabled_next:hover{background:url(../images/forward_enabled_hover.png) no-repeat top right}.paginate_disabled_next{background:url(../images/forward_disabled.png) no-repeat top right}.paging_full_numbers{height:22px;line-height:22px}.paging_full_numbers a:active{outline:0}.paging_full_numbers a:hover{text-decoration:none}.paging_full_numbers a.paginate_active,.paging_full_numbers a.paginate_button{border:1px solid #aaa;-webkit-border-radius:5px;-moz-border-radius:5px;border-radius:5px;padding:2px 5px;margin:0 3px;cursor:pointer;*cursor:hand;color:#333!important}.paging_full_numbers a.paginate_button{background-color:#ddd}.paging_full_numbers a.paginate_button:hover{background-color:#ccc;text-decoration:none!important}.paging_full_numbers a.paginate_active{background-color:#99B3FF}.dataTables_processing{position:absolute;top:50%;left:50%;width:250px;height:30px;margin-left:-125px;margin-top:-15px;padding:14px 0 2px;border:1px solid #ddd;text-align:center;color:#999;font-size:14px;background-color:#fff}.sorting{background:url(../images/sort_both.png) no-repeat center right}.sorting_asc{background:url(../images/sort_asc.png) no-repeat center right}.sorting_desc{background:url(../images/sort_desc.png) no-repeat center right}.sorting_asc_disabled{background:url(../images/sort_asc_disabled.png) no-repeat center right}.sorting_desc_disabled{background:url(../images/sort_desc_disabled.png) no-repeat center right}table.dataTable thead td:active,table.dataTable thead th:active{outline:0}.dataTables_scroll{clear:both}.dataTables_scrollBody{*margin-top:-1px;-webkit-overflow-scrolling:touch}
          </style>

        </head>

        <body>
          <h1><img style='vertical-align:middle;' src=\"http://databases.tangerinecentral.org/tangerine/_design/ojai/images/corner_logo.png\" title=\"Go to main screen.\"> Kenya National Tablet Programme</h1>

          #{contentHtml}
          <p><a href='http://databases.tangerinecentral.org/_csv/report/#{group}/#{workflowIds}/#{year}/#{month}/#{county}.html'>View map and details</a></p>
        </body>
      </html>
    "

    premailer = Premailer.new(html, 
      :with_html_string => true, 
      :warn_level => Premailer::Warnings::SAFE
    )
    mailHtml = premailer.to_inline_css

    if county.downcase != "all"
      emailSubject = "Report for #{county.capitalize} county"
    else
      emailSubject = "County report"
    end

    if email
      
      mail = Mail.deliver do
        to      email
        from    'Tablets Programme <no-reply@tangerinecentral.org>'
        subject emailSubject

        html_part do
          content_type 'text/html; charset=UTF-8'
          body mailHtml
        end
      end

    end

    if format == "json"
      return { 'sent' => true }.to_json
    else
      mailHtml
    end

  end

end