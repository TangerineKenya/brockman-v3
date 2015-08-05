def countyTranslate(zone)
  if zone == "thika"
    return "kiambu"
  else
    return zone
  end
end

def titleize(str)
  return str.split(/ |\_/).map(&:capitalize).join(" ").gsub("Apbet", "APBET")
end