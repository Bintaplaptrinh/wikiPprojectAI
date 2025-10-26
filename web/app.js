const statusBox = document.getElementById('status');
const reportOutput = document.getElementById('report-output');
const generateButton = document.getElementById('generate-report-btn');

function setStatus(message, isError = false) {
  statusBox.textContent = message;
  statusBox.style.color = isError ? '#c1121f' : '#0b5394';
}

function formatNumber(value, maximumFractionDigits = 0) {
  if (!Number.isFinite(value)) {
    return 'N/A';
  }
  return value.toLocaleString('vi-VN', { maximumFractionDigits });
}

function renderSampleTable(records) {
  if (!Array.isArray(records) || records.length === 0) {
    return '<p>Không có dữ liệu mẫu.</p>';
  }

  const rows = records.slice(0, 10).map((record) => (
    `<tr>
      <td>${record.Country || 'N/A'}</td>
      <td>${record.Region || 'N/A'}</td>
      <td>${formatNumber(Number(record.population))}</td>
      <td>${formatNumber(Number(record.area_km2), 2)}</td>
      <td>${formatNumber(Number(record.density), 2)}</td>
    </tr>`
  ));

  return `
    <table class="data-table">
      <thead>
        <tr>
          <th>Quốc gia</th>
          <th>Khu vực</th>
          <th>Dân số</th>
          <th>Diện tích (km²)</th>
          <th>Mật độ</th>
        </tr>
      </thead>
      <tbody>${rows.join('')}</tbody>
    </table>
  `;
}

function renderQ1(results) {
  if (!Array.isArray(results) || results.length === 0) {
    return '<p>Chưa có kết quả từ Hadoop Q1.</p>';
  }
  const rows = results.map((item, index) => (
    `<tr>
      <td>${index + 1}</td>
      <td>${item.region}</td>
      <td>${formatNumber(Number(item.population))}</td>
    </tr>`
  ));
  return `
    <table class="data-table">
      <thead>
        <tr>
          <th>STT</th>
          <th>Khu vực</th>
          <th>Tổng dân số</th>
        </tr>
      </thead>
      <tbody>${rows.join('')}</tbody>
    </table>
  `;
}

function renderQ2(results) {
  if (!Array.isArray(results) || results.length === 0) {
    return '<p>Chưa có kết quả từ Hadoop Q2.</p>';
  }
  const rows = results.map((item, index) => (
    `<tr>
      <td>${index + 1}</td>
      <td>${item.country}</td>
      <td>${formatNumber(Number(item.density), 2)}</td>
      <td>${formatNumber(Number(item.population))}</td>
    </tr>`
  ));
  return `
    <table class="data-table">
      <thead>
        <tr>
          <th>STT</th>
          <th>Quốc gia</th>
          <th>Mật độ</th>
          <th>Dân số</th>
        </tr>
      </thead>
      <tbody>${rows.join('')}</tbody>
    </table>
  `;
}

function renderQ3(results) {
  if (!Array.isArray(results) || results.length === 0) {
    return '<p>Chưa có kết quả từ Hadoop Q3.</p>';
  }
  const rows = results.map((item) => (
    `<tr>
      <td>${item.bucket}</td>
      <td>${formatNumber(Number(item.count))}</td>
    </tr>`
  ));
  return `
    <table class="data-table">
      <thead>
        <tr>
          <th>Nhóm dân số</th>
          <th>Số lượng quốc gia</th>
        </tr>
      </thead>
      <tbody>${rows.join('')}</tbody>
    </table>
  `;
}

function renderQ4(results) {
  if (!Array.isArray(results) || results.length === 0) {
    return '<p>Chưa có kết quả từ Hadoop Q4.</p>';
  }
  const rows = results.map((item, index) => (
    `<tr>
      <td>${index + 1}</td>
      <td>${item.country}</td>
      <td>${formatNumber(Number(item.in_degree))}</td>
      <td>${formatNumber(Number(item.out_degree))}</td>
      <td>${formatNumber(Number(item.total_degree))}</td>
    </tr>`
  ));
  return `
    <table class="data-table">
      <thead>
        <tr>
          <th>STT</th>
          <th>Quốc gia</th>
          <th>In-degree</th>
          <th>Out-degree</th>
          <th>Tổng độ</th>
        </tr>
      </thead>
      <tbody>${rows.join('')}</tbody>
    </table>
  `;
}

function renderSparkSummary(summary) {
  if (!summary || !Array.isArray(summary.clusters)) {
    return '<p>Chưa có kết quả huấn luyện từ Spark.</p>';
  }

  const rows = summary.clusters.map((cluster) => (
    `<tr>
      <td>${cluster.cluster_id}</td>
      <td>${formatNumber(cluster.country_count)}</td>
      <td>${formatNumber(cluster.center_population, 2)}</td>
      <td>${formatNumber(cluster.center_density, 2)}</td>
      <td>${formatNumber(cluster.center_area_km2, 2)}</td>
    </tr>`
  ));

  return `
    <p>Thuật toán: ${summary.algorithm} | Số cụm (k): ${summary.k}</p>
    <p>Tổng sai số (WSSSE): ${formatNumber(summary.within_set_sum_of_squared_errors, 2)}</p>
    <table class="data-table">
      <thead>
        <tr>
          <th>Cụm</th>
          <th>Số quốc gia</th>
          <th>Trung bình dân số</th>
          <th>Trung bình mật độ</th>
          <th>Trung bình diện tích</th>
        </tr>
      </thead>
      <tbody>${rows.join('')}</tbody>
    </table>
  `;
}

function generateReport(data) {
  const { populationSample, totalRecords, q1, q2, q3, q4, sparkSummary } = data;
  const sections = [];

  sections.push('<div class="report-header">');
  sections.push('<h2>BÁO CÁO DÂN SỐ TOÀN CẦU</h2>');
  sections.push(`<p class="report-date">Ngày tạo: ${new Date().toLocaleString('vi-VN')}</p>`);
  sections.push('</div>');

  sections.push('<div class="report-section">');
  sections.push(`<h3>I. Tổng quan dữ liệu MongoDB</h3>`);
  sections.push(`<p><strong>Tổng số quốc gia:</strong> ${formatNumber(Number(totalRecords))}</p>`);
  sections.push('<h4>Mẫu dữ liệu (10 bản ghi đầu tiên)</h4>');
  sections.push(renderSampleTable(populationSample));
  sections.push('</div>');

  sections.push('<div class="report-section">');
  sections.push('<h3>II. Kết quả Hadoop MapReduce</h3>');
  sections.push('<h4>Q1 - Tổng dân số theo khu vực</h4>');
  sections.push(renderQ1(q1?.results));
  sections.push('<h4>Q2 - Top 10 quốc gia có mật độ cao nhất</h4>');
  sections.push(renderQ2(q2?.results));
  sections.push('<h4>Q3 - Nhóm dân số</h4>');
  sections.push(renderQ3(q3?.results));
  sections.push('<h4>Q4 - Chỉ số ảnh hưởng từ đồ thị liên kết</h4>');
  sections.push(renderQ4(q4?.results));
  sections.push('</div>');

  sections.push('<div class="report-section">');
  sections.push('<h3>III. Mô hình máy học với Spark</h3>');
  sections.push(renderSparkSummary(sparkSummary));
  sections.push('</div>');

  sections.push('<div class="report-footer"><p><strong>Ghi chú:</strong> Quy trình gồm Python scraping, Hadoop MapReduce và Spark MLlib.</p></div>');

  reportOutput.innerHTML = sections.join('');
}

async function loadReport() {
  try {
    setStatus('Đang tải báo cáo...', false);
    reportOutput.innerHTML = '<p class="loading">Đang tổng hợp dữ liệu...</p>';

    const response = await fetch('/api/report');
    if (!response.ok) {
      throw new Error('Không lấy được dữ liệu từ máy chủ.');
    }

    const payload = await response.json();
    if (!payload.success) {
      throw new Error(payload.error || 'Báo cáo chưa sẵn sàng.');
    }

    generateReport(payload.data);
    setStatus('Báo cáo đã sẵn sàng.', false);
  } catch (error) {
    console.error(error);
    setStatus('Không thể tạo báo cáo. Vui lòng kiểm tra lại.', true);
    reportOutput.innerHTML = `<p class="error">Lỗi: ${error.message}</p>`;
  }
}

generateButton.addEventListener('click', loadReport);

document.addEventListener('DOMContentLoaded', () => {
  setStatus('Sẵn sàng tạo báo cáo.', false);
});
