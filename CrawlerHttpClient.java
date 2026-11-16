import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CrawlerHttpClient implements AutoCloseable {

    private static final Pattern LINK_PATTERN = Pattern.compile(
            "<a\\b[^>]*?href\\s*=\\s*([\"'])(.*?)\\1[^>]*>(.*?)</a>",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    private final HttpClient delegate;

	private URI uriBase;
	private String baseDomain;
	private String startUrl;

    public CrawlerHttpClient(String urlBase) throws Exception {
        this.delegate = HttpClient.newBuilder()
                .cookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_ALL))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(10))
                .build();

		Optional<URI> resolvedStartUri = resolveInitialUri(urlBase);
		if (resolvedStartUri.isEmpty()) {
			System.err.println("Can't connect to '" + urlBase + "', HTTPS nor HTTP.");
			throw new Exception();
		}

		uriBase = resolvedStartUri.get();
		baseDomain = normalizeDomain(uriBase.getHost());
		startUrl = cleanFragment(uriBase).toString();
    }
	
	@Override
    public void close() {
        this.delegate.close();
    }
	
	public String getStartUrl() {
		return startUrl;
	}
	
	public Optional<URI> resolveInitialUri(String input) {
        String cleanInput = input.trim();

        if (cleanInput.startsWith("http://") || cleanInput.startsWith("https://")) {
            URI explicitUri = URI.create(cleanInput);
            return canConnect(explicitUri) ? Optional.of(explicitUri) : Optional.empty();
        }

        // First try https://
        URI httpsUri = URI.create("https://" + cleanInput);
        //System.out.println("Testing connection to: " + httpsUri);
        if (canConnect(httpsUri)) {
            return Optional.of(httpsUri);
        }

        // Then http://
        URI httpUri = URI.create("http://" + cleanInput);
        //System.out.println("HTTPS failed. Retry with: " + httpUri);
        if (canConnect(httpUri)) {
            return Optional.of(httpUri);
        }

        return Optional.empty();
    }

    public boolean canConnect(URI uri) {
        try {
            HttpRequest request = buildBrowserRequest(uri);
            HttpResponse<Void> response = delegate.send(request, HttpResponse.BodyHandlers.discarding());
            return response.statusCode() >= 200 && response.statusCode() < 400;
        } catch (Exception e) {
            System.err.println("  Failed to connect to " + uri + ": " + e.getMessage());
            return false;
        }
    }

    public HttpRequest buildBrowserRequest(URI uri) {
        return HttpRequest.newBuilder(uri)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
                .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .header("Accept-Language", "en-US,en;q=0.9")
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
    }

    public List<PageLink> fetchAndExtractLinks(URI pageUri) {
        try {
            HttpRequest request = buildBrowserRequest(pageUri);
            HttpResponse<String> response = delegate.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                System.err.println("  Skipped (HTTP " + response.statusCode() + "): " + pageUri);
                return List.of();
            }

            String contentType = response.headers().firstValue("Content-Type").orElse("");
            if (!contentType.contains("text/html")) {
                return List.of();
            }

            return LINK_PATTERN.matcher(response.body())
                    .results()
                    .map(match -> parseLink(pageUri, match.group(2), match.group(3)))
                    .flatMap(Optional::stream)
                    .distinct()
                    .toList();

        } catch (Exception e) {
            System.err.println("  Failed to fetch " + pageUri + ": " + e.getMessage());
            return List.of();
        }
    }

    public Optional<PageLink> parseLink(URI base, String rawHref, String rawInnerHtml) {
        String href = rawHref.trim();
        if (isNotAPageLink(href) || isAssetFile(href)) {
            return Optional.empty();
        }
		
        String cleanLabel = rawInnerHtml.replaceAll("<[^>]*>", " ")
                .replaceAll("\\s+", " ")
                .trim();

        if (cleanLabel.isEmpty()) {
            cleanLabel = "[No Text / Icon]";
        }

        try {
            URI linkURI = base.resolve(href);
            String linkDomain = normalizeDomain(linkURI.getHost());

            if (linkDomain != null && linkDomain.equalsIgnoreCase(baseDomain)) {
                URI cleanURI = cleanFragment(linkURI);
                return Optional.of(new PageLink(cleanLabel, cleanURI.toString()));
            }
        } catch (Exception ignored) {
        }
        return Optional.empty();
    }

    public URI cleanFragment(URI uri) throws Exception {
        return new URI(
                uri.getScheme(),
                uri.getAuthority(),
                uri.getPath(),
                uri.getQuery(),
                null
        );
    }

    public boolean isNotAPageLink(String href) {
        return href.isEmpty() || href.startsWith("#") || href.startsWith("javascript:") 
				|| href.startsWith("mailto:") || href.startsWith("tel:");
    }
	
	public boolean isAssetFile(String url) {
        String lower = url.toLowerCase();
        return lower.endsWith(".pdf") || lower.endsWith(".jpg") || lower.endsWith(".png")
                || lower.endsWith(".svg") || lower.endsWith(".zip") || lower.endsWith(".xml");
    }

    public String normalizeDomain(String host) {
        if (host == null) return null;
        return host.startsWith("www.") ? host.substring(4) : host;
    }
	
}