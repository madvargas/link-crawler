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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VirtualThreadCrawlerJdk21 {

    private static final Pattern LINK_PATTERN = Pattern.compile(
            "<a\\b[^>]*?href\\s*=\\s*([\"'])(.*?)\\1[^>]*>(.*?)</a>",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    private static final int MAX_PAGES_TO_CRAWL = 20;
    private static final int MAX_CONCURRENT_REQUESTS = 5;

    public record PageLink(String label, String url) {}

    public static void main(String[] args) {
		long inicio = System.nanoTime();
        String urlBase = args[0];
        CookieManager cookieManager = new CookieManager(null, CookiePolicy.ACCEPT_ALL);

        Set<String> visitedUrls = ConcurrentHashMap.newKeySet();
        Set<PageLink> allCollectedLinks = ConcurrentHashMap.newKeySet();
        AtomicInteger crawledCount = new AtomicInteger(0);

        Semaphore concurrencyLimit = new Semaphore(MAX_CONCURRENT_REQUESTS);
        Phaser activeTasks = new Phaser(1);

        try (HttpClient client = HttpClient.newBuilder()
                .cookieHandler(cookieManager)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

            Optional<URI> resolvedStartUri = resolveInitialUri(client, urlBase);
            if (resolvedStartUri.isEmpty()) {
                System.err.println("Can't connect to '" + urlBase + "', HTTPS nor HTTP.");
                return;
            }

            URI uriBase = resolvedStartUri.get();
            String baseDomain = normalizeDomain(uriBase.getHost());
            String startUrl = cleanFragment(uriBase).toString();

            if (visitedUrls.add(startUrl)) {
                submitCrawlTask(startUrl, client, baseDomain, executor, activeTasks,
                        concurrencyLimit, visitedUrls, allCollectedLinks, crawledCount);
            }

            activeTasks.arriveAndAwaitAdvance();

            List<PageLink> sortedLinks = allCollectedLinks.stream()
                    .sorted(Comparator.comparing(PageLink::label, String.CASE_INSENSITIVE_ORDER)
                            .thenComparing(PageLink::url))
                    .toList();

			long fin = System.nanoTime();
			long duracionNano = fin - inicio;
			double duracionMs = duracionNano / 1_000_000.0;
			//System.out.println("Time in milliseconds: " + duracionMs + " ms");

            System.out.println("\n=== Total unique links sorted by label (" + sortedLinks.size() + ") ===");
            printResults(sortedLinks);

        } catch (Exception e) {
            System.err.println("Crawler error: " + e.getMessage());
        }
    }
	
	private static Optional<URI> resolveInitialUri(HttpClient client, String input) {
        String cleanInput = input.trim();

        if (cleanInput.startsWith("http://") || cleanInput.startsWith("https://")) {
            URI explicitUri = URI.create(cleanInput);
            return canConnect(client, explicitUri) ? Optional.of(explicitUri) : Optional.empty();
        }

        // First try https://
        URI httpsUri = URI.create("https://" + cleanInput);
        //System.out.println("Testing connection to: " + httpsUri);
        if (canConnect(client, httpsUri)) {
            return Optional.of(httpsUri);
        }

        // Then http://
        URI httpUri = URI.create("http://" + cleanInput);
        //System.out.println("HTTPS failed. Retry with: " + httpUri);
        if (canConnect(client, httpUri)) {
            return Optional.of(httpUri);
        }

        return Optional.empty();
    }

    private static boolean canConnect(HttpClient client, URI uri) {
        try {
            HttpRequest request = buildBrowserRequest(uri);
            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
            return response.statusCode() >= 200 && response.statusCode() < 400;
        } catch (Exception e) {
            System.err.println("  Failed to connect to " + uri + ": " + e.getMessage());
            return false;
        }
    }

    private static HttpRequest buildBrowserRequest(URI uri) {
        return HttpRequest.newBuilder(uri)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
                .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .header("Accept-Language", "en-US,en;q=0.9")
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
    }

	private static void printResults(List<PageLink> sortedLinks) {
		for (PageLink link : sortedLinks) {
			System.out.printf("%-45s -> %s%n", link.label(), link.url());
		}
	}

    private static void submitCrawlTask(
            String url,
            HttpClient client,
            String baseDomain,
            ExecutorService executor,
            Phaser activeTasks,
            Semaphore concurrencyLimit,
            Set<String> visitedUrls,
            Set<PageLink> allCollectedLinks,
            AtomicInteger crawledCount) {

        if (crawledCount.incrementAndGet() > MAX_PAGES_TO_CRAWL) {
            return;
        }

        activeTasks.register();

        executor.submit(() -> {
            try {
                concurrencyLimit.acquire();
                try {
                    //System.out.printf("[%s] Crawling: %s%n", Thread.currentThread(), url);
                    List<PageLink> pageLinks = fetchAndExtractLinks(client, URI.create(url), baseDomain);
                    allCollectedLinks.addAll(pageLinks);

                    for (PageLink link : pageLinks) {
                        if (crawledCount.get() < MAX_PAGES_TO_CRAWL && visitedUrls.add(link.url())) {
                            submitCrawlTask(link.url(), client, baseDomain, executor, activeTasks,
                                    concurrencyLimit, visitedUrls, allCollectedLinks, crawledCount);
                        }
                    }

                    //Thread.sleep(200);
                } finally {
                    concurrencyLimit.release();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                activeTasks.arriveAndDeregister();
            }
        });
    }

    private static List<PageLink> fetchAndExtractLinks(HttpClient client, URI pageUri, String dominioBase) {
        try {
            HttpRequest request = buildBrowserRequest(pageUri);
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

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
                    .map(match -> parseLink(pageUri, dominioBase, match.group(2), match.group(3)))
                    .flatMap(Optional::stream)
                    .distinct()
                    .toList();

        } catch (Exception e) {
            System.err.println("  Failed to fetch " + pageUri + ": " + e.getMessage());
            return List.of();
        }
    }

    private static Optional<PageLink> parseLink(URI base, String baseDomain, String rawHref, String rawInnerHtml) {
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

    private static URI cleanFragment(URI uri) throws Exception {
        return new URI(
                uri.getScheme(),
                uri.getAuthority(),
                uri.getPath(),
                uri.getQuery(),
                null
        );
    }

    private static boolean isNotAPageLink(String href) {
        return href.isEmpty() || href.startsWith("#") || href.startsWith("javascript:") 
				|| href.startsWith("mailto:") || href.startsWith("tel:");
    }
	
	private static boolean isAssetFile(String url) {
        String lower = url.toLowerCase();
        return lower.endsWith(".pdf") || lower.endsWith(".jpg") || lower.endsWith(".png")
                || lower.endsWith(".svg") || lower.endsWith(".zip") || lower.endsWith(".xml");
    }

    private static String normalizeDomain(String host) {
        if (host == null) return null;
        return host.startsWith("www.") ? host.substring(4) : host;
    }
	
}