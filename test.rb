require "bundler"
Bundler.require

mail = Mail.deliver do
  to      "fetmar@gmail.com"
  from    'Tablets Programme <no-reply@tangerinecentral.org>'
  subject "This is a test email"

  html_part do
    content_type 'text/html; charset=UTF-8'
    body "hi fet!"
  end
end
