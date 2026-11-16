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

public class VirtualThreadCrawlerJdk21 {

    private static final int MAX_PAGES_TO_CRAWL = 20;
    private static final int MAX_CONCURRENT_REQUESTS = 5;

    public static void main(String[] args) {
		long inicio = System.nanoTime();
        String urlBase = args[0];

        Set<String> visitedUrls = ConcurrentHashMap.newKeySet();
        Set<PageLink> allCollectedLinks = ConcurrentHashMap.newKeySet();
        AtomicInteger crawledCount = new AtomicInteger(0);

        Semaphore concurrencyLimit = new Semaphore(MAX_CONCURRENT_REQUESTS);
        Phaser activeTasks = new Phaser(1);

        try (CrawlerHttpClient client = new CrawlerHttpClient(urlBase);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

            if (visitedUrls.add(client.getStartUrl())) {
                submitCrawlTask(client.getStartUrl(), client, executor, activeTasks, concurrencyLimit,
						visitedUrls, allCollectedLinks, crawledCount);
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
	
	private static void printResults(List<PageLink> sortedLinks) {
		for (PageLink link : sortedLinks) {
			System.out.printf("%-45s -> %s%n", link.label(), link.url());
		}
	}

    private static void submitCrawlTask(
            String url,
            CrawlerHttpClient client,
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
                    List<PageLink> pageLinks = client.fetchAndExtractLinks(url);
                    allCollectedLinks.addAll(pageLinks);

                    for (PageLink link : pageLinks) {
                        if (crawledCount.get() < MAX_PAGES_TO_CRAWL && visitedUrls.add(link.url())) {
                            submitCrawlTask(link.url(), client, executor, activeTasks,
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

}
