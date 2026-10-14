// Run against tools/preview_ofs_repeated_orders_local.py. Requires Playwright.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require('playwright');

(async () => {
  const browser = await chromium.launch({headless: true, channel: 'chrome'});
  const page = await browser.newPage({viewport: {width: 1366, height: 768}});
  const errors = [], requests = [];
  page.on('pageerror', error => errors.push(error.message));
  page.on('request', request => requests.push({url:request.url(), method:request.method()}));
  page.on('dialog', async dialog => {errors.push(`Unexpected dialog: ${dialog.message()}`); await dialog.dismiss();});
  const base = 'http://127.0.0.1:5097';
  const dir = path.resolve(__dirname, '../temp_uploads/repeated-orders-qa');
  fs.mkdirSync(dir, {recursive:true});
  try {
    await page.goto(`${base}/fixture-session`);
    await page.locator('#ro-count').filter({hasText:'38 contratos'}).waitFor();
    const initialData = (await (await page.request.get(`${base}/ofs/os-repetidas/data`)).json()).data;
    assert.equal(await page.locator('.ro-case').count(), 30);
    assert.equal(await page.locator('#repeated-orders select').count(), 0);
    assert.equal(await page.locator('#ro-total-count').textContent(), '81');
    assert.equal(await page.locator('#ro-count').textContent(), '38 contratos · 81 atividades');
    assert.equal(await page.locator('[data-severity="purple"] .ro-symbol').textContent(), '3+');
    assert.equal(await page.locator('[data-severity="yellow"] .ro-symbol').textContent(), '≠');
    assert.equal((await page.locator('[data-severity="red"] span').textContent()).trim(), '');
    const initialPosts = requests.filter(request => request.method === 'POST').length;
    assert.equal(await page.locator('#ro-page').textContent(), '1 / 2');
    assert.equal(await page.locator('.ro-case-meta').count(), 0);
    assert.ok(!(await page.locator('.ro-case summary').first().textContent()).includes('Date'));
    assert.ok(!(await page.locator('.ro-case summary').first().textContent()).includes('ResourceId'));
    assert.ok((await page.locator('.ro-case').first().boundingBox()).height <= 40, 'Collapsed row is not compact');
    assert.ok(await page.locator('#repeated-orders').evaluate(root => [...root.querySelectorAll('button,input,details,.om-card,.om-table-card')].every(node => parseFloat(getComputedStyle(node).borderRadius)>0)), 'Monitor rounding must be preserved');
    // Compare after the Monitor's selection transition has completed.
    await page.locator('#ro-total').evaluate(card => card.getAnimations().forEach(animation => animation.finish()));
    const cardComparison = await page.locator('#ro-total').evaluate(card => {
      const reference = document.createElement('section'); reference.className='om-kpis';
      reference.style.cssText='position:fixed;visibility:hidden;pointer-events:none';
      const button = document.createElement('button'); button.className='is-active';
      reference.append(button); card.closest('main').append(reference);
      const props=['display','minHeight','padding','borderRadius','borderWidth','backgroundColor','boxShadow','fontSize','textAlign'];
      const actual=getComputedStyle(card), expected=getComputedStyle(button);
      const differences=props.filter(prop=>actual[prop]!==expected[prop]);reference.remove();return differences;
    });
    assert.deepEqual(cardComparison, [], 'Summary card must use the Monitor card model');
    assert.ok(await page.locator('.sidebar-icon').first().evaluate(node => parseFloat(getComputedStyle(node).borderRadius)>0), 'OS repeated page must preserve sidebar rounding');
    assert.equal(await page.locator('#ro-progress').isVisible(), false);
    await page.locator('.ro-case summary').first().click();
    await page.locator('.ro-case table').first().waitFor();
    assert.ok((await page.locator('.ro-case table').first().textContent()).includes('ResourceId'));
    assert.ok((await page.locator('.ro-case table').first().textContent()).includes('Date'));
    const expectedActivities = initialData.payload.contracts[0].orders.flatMap(order=>order.activities);
    const tableRows = page.locator('.ro-case table').first().locator('tbody tr');
    for (let index=0;index<expectedActivities.length;index++) {
      assert.equal(await tableRows.nth(index).locator('td').nth(5).textContent(), expectedActivities[index].date);
      assert.equal(await tableRows.nth(index).locator('td').nth(7).textContent(), expectedActivities[index].resourceId);
      assert.equal(await tableRows.nth(index).locator('td').nth(8).textContent(), expectedActivities[index].regional || 'Sem estado');
      assert.equal(await tableRows.nth(index).locator('td').nth(9).textContent(), expectedActivities[index].timeSlot);
    }
    assert.ok(new Set(expectedActivities.map(a=>a.date)).size > 1);
    assert.ok(new Set(expectedActivities.map(a=>a.resourceId)).size > 1);
    // Read-only polling must preserve expansion and only call local snapshot.
    await page.waitForTimeout(10500);
    assert.equal(await page.locator('.ro-case[open]').count(), 1);
    await page.screenshot({path:path.join(dir, 'notebook-1366.png'), fullPage:false});
    await page.locator('#ro-next').click();
    assert.equal(await page.locator('.ro-case').count(), 8);
    await page.locator('[data-severity="purple"]').click();
    assert.equal(await page.locator('.ro-case').count(), 4);
    assert.equal(await page.locator('#ro-count').textContent(), '4 contratos · 13 atividades');
    await page.locator('[data-severity="red"]').click();
    assert.equal(await page.locator('#ro-count').textContent(), '22 contratos · 49 atividades');
    await page.locator('[data-severity="purple"]').click();
    assert.equal(await page.locator('.ro-case').count(), 18);
    assert.equal(await page.locator('.ro-case .ro-badge').count(), 0);
    await page.locator('#ro-types button[data-type="MAN"]').click();
    assert.equal(await page.locator('.ro-case').count(), 0);
    assert.equal(await page.locator('#ro-total-count').textContent(), '81');
    await page.locator('#ro-types button[data-type="INST"]').click();
    assert.equal(await page.locator('.ro-case').count(), 18);
    await page.locator('#ro-types button[data-type=""]').click();
    assert.equal(await page.locator('#ro-types button[aria-pressed="true"]').textContent(), 'Todos');
    await page.locator('#ro-total').click();
    await page.locator('[data-severity="yellow"]').focus();
    await page.keyboard.press('Enter');
    assert.equal(await page.locator('.ro-case').count(), 16);
    assert.equal(await page.locator('.ro-case .ro-badge').first().textContent(), '≠');
    assert.equal(await page.locator('[data-severity="yellow"]').getAttribute('aria-pressed'), 'true');
    await page.locator('#ro-total').click();
    assert.equal(requests.filter(request => request.method === 'POST').length, initialPosts);
    const beforeRegionalRequests = requests.length;
    await page.locator('#ro-regionals button[data-regional="Sem estado"]').click();
    assert.equal(await page.locator('#ro-count').textContent(), '1 contratos · 2 atividades');
    await page.locator('#ro-regionals button[data-regional="RS"]').click();
    assert.equal(await page.locator('#ro-count').textContent(), '38 contratos · 81 atividades');
    await page.locator('#ro-regionals button[data-regional="Sem estado"]').click();
    assert.equal(await page.locator('#ro-count').textContent(), '37 contratos · 79 atividades');
    await page.locator('.ro-case summary').first().click();
    await page.locator('.ro-case table').first().waitFor();
    assert.ok((await page.locator('.ro-case table').first().textContent()).includes('MG'), 'Other region OS must remain visible for context');
    await page.locator('[data-severity="red"]').click();
    assert.equal(await page.locator('#ro-count').textContent(), '17 contratos · 34 atividades');
    await page.locator('#ro-types button[data-type="MAN"]').click();
    assert.equal(await page.locator('.ro-case').count(), 0);
    assert.equal(requests.length, beforeRegionalRequests, 'Regional filters must not fetch');
    await page.locator('#ro-regionals button[data-regional="MG"]').click();
    assert.equal(await page.locator('#ro-regionals button[aria-pressed="true"]').count(), 2);
    await page.locator('#ro-clear-regionals').click();
    assert.equal(await page.locator('#ro-regionals button[aria-pressed="true"]').textContent(), 'Todos');
    assert.equal(await page.locator('[data-severity="red"]').getAttribute('aria-pressed'), 'true');
    assert.equal(await page.locator('#ro-types button[data-type="MAN"]').getAttribute('aria-pressed'), 'true');
    assert.equal(requests.length, beforeRegionalRequests, 'Clearing states must not fetch');
    await page.locator('#ro-total').click();
    assert.equal(await page.locator('#ro-regionals button[aria-pressed="true"]').textContent(), 'Todos');
    await page.locator('#ro-contract').fill('<img');
    assert.equal(await page.locator('.ro-case').count(), 1);
    assert.equal(await page.locator('#ro-list img').count(), 0);
    assert.ok((await page.locator('.ro-case summary').textContent()).includes('<img src=x onerror=alert(1)>'));
    await page.locator('#ro-contract').fill('does-not-exist');
    assert.equal(await page.locator('.ro-case').count(), 0);
    await page.locator('#ro-contract').fill('');
    await page.locator('#ro-total').click();
    const viewportChecks = [];
    for (const [width, height] of [[1366,768],[1280,720],[1024,768],[390,844]]) {
      await page.setViewportSize({width,height});
      if (width === 390) await page.locator('[data-sidebar-toggle]').click();
      await page.waitForTimeout(250);
      const dimensions = await page.evaluate(() => ({viewport:window.innerWidth, content:document.documentElement.scrollWidth,
        summary:document.querySelector('.ro-summary').getBoundingClientRect().width,
        maxCardRight:Math.max(...[...document.querySelectorAll('.ro-stat')].map(node=>node.getBoundingClientRect().right))}));
      assert.ok(dimensions.content <= dimensions.viewport, `Overflow at ${width}: ${JSON.stringify(dimensions)}`);
      assert.ok(dimensions.maxCardRight <= dimensions.viewport, `Summary clipped at ${width}`);
      viewportChecks.push({width,height,...dimensions});
    }
    await page.screenshot({path:path.join(dir, 'mobile-390.png'), fullPage:false});
    // CSRF and server errors must be visible; refresh disabled for pending work.
    await page.route('**/ofs/os-repetidas/refresh', route => route.fulfill({status:409, contentType:'application/json',body:JSON.stringify({ok:false,error:{message:'Aguarde o intervalo global de 10 minutos.'}})}));
    await page.locator('#ro-refresh').click();
    await page.locator('#ro-error').filter({hasText:'Aguarde o intervalo global'}).waitFor();
    assert.equal(await page.locator('#ro-progress').isVisible(), false);
    await page.unroute('**/ofs/os-repetidas/refresh');
    let simulatedStatus='queued', simulatedId=2, publishedId=1;
    await page.route('**/ofs/os-repetidas/refresh', async route => {
      await new Promise(resolve=>setTimeout(resolve,300));
      await route.fulfill({status:202,contentType:'application/json',body:JSON.stringify({ok:true,data:{id:simulatedId,status:'queued'}})});
    });
    await page.route('**/ofs/os-repetidas/data', route => route.fulfill({status:200,contentType:'application/json',body:JSON.stringify({ok:true,data:{...initialData,run:{id:simulatedId,status:simulatedStatus,pages:2,received:2500,heartbeat_at:initialData.published_at,error_text:'Falha simulada'},remaining_seconds:0,snapshot_run_id:publishedId}})}));
    await page.locator('#ro-refresh').click();
    await page.locator('#ro-progress-title').filter({hasText:'Solicitando atualização'}).waitFor();
    assert.equal(await page.locator('#ro-progress').isVisible(), true);
    assert.equal(await page.locator('#ro-progress [role="progressbar"]').getAttribute('aria-valuenow'), null);
    await page.locator('#ro-progress-title').filter({hasText:'Atualização na fila'}).waitFor();
    await page.locator('#ro-refresh').filter({hasText:'Atualização em andamento'}).waitFor();
    assert.equal(await page.locator('#ro-refresh').isDisabled(), true);
    simulatedStatus='running';
    await page.locator('#ro-progress-detail').filter({hasText:'2 páginas · 2500 atividades'}).waitFor();
    await page.setViewportSize({width:1366,height:768});
    await page.locator('[data-sidebar-toggle]').click();
    await page.screenshot({path:path.join(dir,'loading-1366.png'),fullPage:false});
    simulatedStatus='succeeded';publishedId=2;
    await page.locator('#ro-progress').waitFor({state:'hidden'});
    assert.equal(await page.locator('#ro-refresh').isDisabled(), false);
    simulatedStatus='queued';simulatedId=3;
    await page.locator('#ro-refresh').click();
    await page.locator('#ro-progress-title').filter({hasText:'Atualização na fila'}).waitFor();
    simulatedStatus='failed';
    await page.locator('#ro-error').filter({hasText:'Falha simulada'}).waitFor();
    assert.equal(await page.locator('#ro-progress').isVisible(), false);
    assert.equal(errors.length, 0, errors.join('\n'));
    assert.ok(requests.every(request => request.url.startsWith(base)), 'Unexpected external request');
    const report = {ok:true, viewportChecks, browserErrors:errors, requests:requests.map(r=>({path:new URL(r.url).pathname,method:r.method}))};
    fs.writeFileSync(path.join(dir,'report.json'), JSON.stringify(report,null,2));
    console.log(JSON.stringify(report));
  } finally {
    await browser.close();
  }
})().catch(error => {console.error(error); process.exitCode=1;});
