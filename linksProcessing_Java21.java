import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class AsyncCrawlerJdk21 {

    private static final Pattern LINK_PATTERN = Pattern.compile(
            "<a\\b[^>]*?href\\s*=\\s*([\"'])(.*?)\\1[^>]*>(.*?)</a>",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    private static final int MAX_PAGES_TO_CRAWL = 20;
    private static final int MAX_CONCURRENT_REQUESTS = 5;

    public record PageLink(String label, String url) {}

    public static void main(String[] args) {
		long inicio = System.nanoTime();
        String urlBase = "https://" + args[0];
        CookieManager cookieManager = new CookieManager(null, CookiePolicy.ACCEPT_ALL);

        Set<String> visitedUrls = ConcurrentHashMap.newKeySet();
        Set<PageLink> allCollectedLinks = ConcurrentHashMap.newKeySet();
        AtomicInteger crawledCount = new AtomicInteger(0);
        Semaphore concurrencyLimit = new Semaphore(MAX_CONCURRENT_REQUESTS);

        Executor virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();

        try (HttpClient client = HttpClient.newBuilder()
                .cookieHandler(cookieManager)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(10))
                .executor(virtualExecutor)
                .build()) {

            URI uriBase = URI.create(urlBase);
            String dominioBase = normalizarDominio(uriBase.getHost());
            String startUrl = limpiarFragmento(uriBase).toString();

            if (visitedUrls.add(startUrl)) {
                crawlAsync(startUrl, client, dominioBase, virtualExecutor,
                        concurrencyLimit, visitedUrls, allCollectedLinks, crawledCount).join();
            }

            List<PageLink> sortedLinks = allCollectedLinks.stream()
                    .sorted(Comparator.comparing(PageLink::label, String.CASE_INSENSITIVE_ORDER)
                            .thenComparing(PageLink::url))
                    .toList();

			long fin = System.nanoTime();
			long duracionNano = fin - inicio;
			double duracionMs = duracionNano / 1_000_000.0;
			System.out.println("Tiempo en milisegundos: " + duracionMs + " ms");

            System.out.println("\n=== Total unique links sorted by label (" + sortedLinks.size() + ") ===");
			//printResults(sortedLinks);

        } catch (Exception e) {
            System.err.println("Crawler error: " + e.getMessage());
        }
    }

    private static void printResults(List<PageLink> sortedLinks) {
		for (PageLink link : sortedLinks) {
			System.out.printf("%-45s -> %s%n", link.label(), link.url());
		}
	}
	
    private static CompletableFuture<Void> crawlAsync(
            String url,
            HttpClient client,
            String dominioBase,
            Executor executor,
            Semaphore concurrencyLimit,
            Set<String> visitedUrls,
            Set<PageLink> allCollectedLinks,
            AtomicInteger crawledCount) {

        if (crawledCount.incrementAndGet() > MAX_PAGES_TO_CRAWL) {
            return CompletableFuture.completedFuture(null);
        }

        // Adquiere permiso de forma asíncrona antes de lanzar sendAsync
        CompletableFuture<Void> acquireFuture = CompletableFuture.runAsync(() -> {
            try {
                concurrencyLimit.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, executor);

        return acquireFuture
                .thenCompose(ignored -> fetchAndExtractLinksAsync(client, URI.create(url), dominioBase))
                .whenComplete((links, error) -> {
                    // Retrasa 200ms la liberación del permiso sin bloquear hilos usando delayedExecutor
                    CompletableFuture.delayedExecutor(200, TimeUnit.MILLISECONDS, executor)
                            .execute(concurrencyLimit::release);
                })
                .thenCompose(pageLinks -> {
                    allCollectedLinks.addAll(pageLinks);
                    List<CompletableFuture<Void>> childFutures = new ArrayList<>();

                    for (PageLink link : pageLinks) {
                        if (crawledCount.get() < MAX_PAGES_TO_CRAWL
                                && !isAssetFile(link.url())
                                && visitedUrls.add(link.url())) {

                            CompletableFuture<Void> childFuture = crawlAsync(
                                    link.url(), client, dominioBase, executor,
                                    concurrencyLimit, visitedUrls, allCollectedLinks, crawledCount
                            );
                            childFutures.add(childFuture);
                        }
                    }

                    if (childFutures.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    return CompletableFuture.allOf(childFutures.toArray(new CompletableFuture[0]));
                });
    }

    private static CompletableFuture<List<PageLink>> fetchAndExtractLinksAsync(
            HttpClient client, URI pageUri, String dominioBase) {

        //System.out.printf("[%s] Requesting: %s%n", Thread.currentThread(), pageUri);

        HttpRequest request = HttpRequest.newBuilder(pageUri)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
                .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .header("Accept-Language", "en-US,en;q=0.9")
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    if (response.statusCode() != 200) {
                        System.err.println("  Skipped (HTTP " + response.statusCode() + "): " + pageUri);
                        return List.<PageLink>of();
                    }

                    String contentType = response.headers().firstValue("Content-Type").orElse("");
                    if (!contentType.contains("text/html")) {
                        return List.<PageLink>of();
                    }

                    return LINK_PATTERN.matcher(response.body())
                            .results()
                            .map(match -> parseLink(pageUri, dominioBase, match.group(2), match.group(3)))
                            .flatMap(Optional::stream)
                            .distinct()
                            .toList();
                })
                .exceptionally(ex -> {
                    System.err.println("  Failed to fetch " + pageUri + ": " + ex.getMessage());
                    return List.of();
                });
    }

    private static Optional<PageLink> parseLink(URI base, String dominioBase, String rawHref, String rawInnerHtml) {
        String href = rawHref.trim();
        if (href.isEmpty() || href.startsWith("#") || href.startsWith("javascript:") || href.startsWith("mailto:") || href.startsWith("tel:")) {
            return Optional.empty();
        }

        String cleanLabel = rawInnerHtml.replaceAll("<[^>]*>", " ")
                .replaceAll("\\s+", " ")
                .trim();

        if (cleanLabel.isEmpty()) {
            cleanLabel = "[No Text / Icon]";
        }

        try {
            URI uriEnlace = base.resolve(href);
            String dominioEnlace = normalizarDominio(uriEnlace.getHost());

            if (dominioEnlace != null && dominioEnlace.equalsIgnoreCase(dominioBase)) {
                URI uriLimpia = limpiarFragmento(uriEnlace);
                return Optional.of(new PageLink(cleanLabel, uriLimpia.toString()));
            }
        } catch (Exception ignored) {
        }
        return Optional.empty();
    }

    private static URI limpiarFragmento(URI uri) throws Exception {
        return new URI(
                uri.getScheme(),
                uri.getAuthority(),
                uri.getPath(),
                uri.getQuery(),
                null
        );
    }

    private static boolean isAssetFile(String url) {
        String lower = url.toLowerCase();
        return lower.endsWith(".pdf") || lower.endsWith(".jpg") || lower.endsWith(".png")
                || lower.endsWith(".svg") || lower.endsWith(".zip") || lower.endsWith(".xml");
    }

    private static String normalizarDominio(String host) {
        if (host == null) {
            return null;
        }
        return host.startsWith("www.") ? host.substring(4) : host;
    }
}
