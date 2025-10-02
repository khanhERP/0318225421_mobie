
import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "@/lib/i18n";
import * as XLSX from "xlsx";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { POSHeader } from "@/components/pos/header";
import { RightSidebar } from "@/components/ui/right-sidebar";
import IncomeVoucherModal from "@/components/pos/income-voucher-modal";
import ExpenseVoucherModal from "@/components/pos/expense-voucher-modal";
import {
  BookOpen,
  TrendingUp,
  TrendingDown,
  Wallet,
  Calendar,
  FileText,
  Plus,
  Minus,
} from "lucide-react";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";

interface CashBookPageProps {
  onLogout: () => void;
}

interface CashTransaction {
  id: string;
  date: string;
  description: string;
  source: string;
  type: "thu" | "chi";
  amount: number;
  balance: number;
}

export default function CashBookPage({ onLogout }: CashBookPageProps) {
  const { t } = useTranslation();
  
  // Modal states
  const [showIncomeVoucherModal, setShowIncomeVoucherModal] = useState(false);
  const [showExpenseVoucherModal, setShowExpenseVoucherModal] = useState(false);
  const [selectedVoucher, setSelectedVoucher] = useState(null);
  const [voucherMode, setVoucherMode] = useState("create");
  
  // Filters
  const [filterType, setFilterType] = useState("all"); // "all", "thu", "chi"
  const [paymentMethodFilter, setPaymentMethodFilter] = useState("all"); // "all" or specific payment method
  const [startDate, setStartDate] = useState(() => {
    const today = new Date();
    const firstDayOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);
    return firstDayOfMonth.toISOString().split('T')[0];
  });
  const [endDate, setEndDate] = useState(() => {
    return new Date().toISOString().split('T')[0];
  });

  // Query orders (thu - income from sales)
  const { data: orders = [] } = useQuery({
    queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/orders"],
    queryFn: async () => {
      try {
        const response = await fetch("https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/orders");
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        return Array.isArray(data) ? data : [];
      } catch (error) {
        console.error('Error fetching orders:', error);
        return [];
      }
    },
  });

  // Query purchase receipts (chi - expenses from purchases)
  const { data: purchaseReceipts = [] } = useQuery({
    queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/purchase-receipts"],
    queryFn: async () => {
      try {
        const response = await fetch("https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/purchase-receipts");
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        return Array.isArray(data.data) ? data.data : [];
      } catch (error) {
        console.error('Error fetching purchase receipts:', error);
        return [];
      }
    },
  });

  // Query income vouchers (thu - manual income entries)
  const { data: incomeVouchers = [] } = useQuery({
    queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/income-vouchers"],
    queryFn: async () => {
      try {
        const response = await fetch("https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/income-vouchers");
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        return Array.isArray(data) ? data : [];
      } catch (error) {
        console.error('Error fetching income vouchers:', error);
        return [];
      }
    },
  });

  // Query expense vouchers (chi - manual expense entries)
  const { data: expenseVouchers = [] } = useQuery({
    queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/expense-vouchers"],
    queryFn: async () => {
      try {
        const response = await fetch("https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/expense-vouchers");
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        return Array.isArray(data) ? data : [];
      } catch (error) {
        console.error('Error fetching expense vouchers:', error);
        return [];
      }
    },
  });

  // Query suppliers for name mapping
  const { data: suppliers = [] } = useQuery({
    queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/suppliers"],
    queryFn: async () => {
      try {
        const response = await fetch("https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/suppliers");
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        return Array.isArray(data) ? data : [];
      } catch (error) {
        console.error('Error fetching suppliers:', error);
        return [];
      }
    },
  });

  // Query payment methods
  const { data: paymentMethods = [] } = useQuery({
    queryKey: ["https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/payment-methods"],
    queryFn: async () => {
      try {
        const response = await fetch("https://bad07204-3e0d-445f-a72e-497c63c9083a-00-3i4fcyhnilzoc.pike.replit.dev/api/payment-methods");
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        return Array.isArray(data) ? data : [];
      } catch (error) {
        console.error('Error fetching payment methods:', error);
        return [];
      }
    },
  });

  // Calculate cash book data
  const cashBookData = useMemo(() => {
    const transactions: CashTransaction[] = [];
    
    // Add income transactions from orders (thu)
    orders
      .filter(order => order.status === 'paid' || order.paymentStatus === 'paid')
      .forEach(order => {
        const orderDate = new Date(order.orderedAt || order.paidAt);
        transactions.push({
          id: `ORDER-${order.id}`,
          date: orderDate.toISOString().split('T')[0],
          description: `${order.salesChannel === 'table' ? 'Bán tại bàn' : 'Bán hàng'} - ${order.orderNumber}`,
          source: order.customerName || 'Khách hàng',
          type: 'thu',
          amount: parseFloat(order.total || '0'),
          balance: 0, // Will be calculated later
        });
      });

    // Add income transactions from income vouchers (thu)
    incomeVouchers.forEach(voucher => {
      const voucherDate = new Date(voucher.date);
      transactions.push({
        id: `INCOME-VOUCHER-${voucher.id}`,
        date: voucher.date,
        description: `${voucher.category} - ${voucher.voucherNumber}`,
        source: voucher.recipient,
        type: 'thu',
        amount: parseFloat(voucher.amount || '0'),
        balance: 0, // Will be calculated later
      });
    });

    // Add expense transactions from expense vouchers (chi)
    expenseVouchers.forEach(voucher => {
      const voucherDate = new Date(voucher.date);
      transactions.push({
        id: `EXPENSE-VOUCHER-${voucher.id}`,
        date: voucher.date,
        description: `${voucher.category} - ${voucher.voucherNumber}`,
        source: voucher.recipient,
        type: 'chi',
        amount: parseFloat(voucher.amount || '0'),
        balance: 0, // Will be calculated later
      });
    });

    // Add expense transactions from purchase receipts (chi)
    purchaseReceipts.forEach(receipt => {
      const receiptDate = new Date(receipt.purchaseDate || receipt.createdAt);
      const supplier = suppliers.find(s => s.id === receipt.supplierId);
      transactions.push({
        id: `PURCHASE-${receipt.id}`,
        date: receiptDate.toISOString().split('T')[0],
        description: `Mua hàng - ${receipt.receiptNumber}`,
        source: supplier?.name || 'Nhà cung cấp',
        type: 'chi',
        amount: parseFloat(receipt.total || '0'),
        balance: 0, // Will be calculated later
      });
    });

    // Sort by date
    transactions.sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());

    // Filter by date range
    const filteredTransactions = transactions.filter(transaction => {
      const transactionDate = transaction.date;
      return transactionDate >= startDate && transactionDate <= endDate;
    });

    // Calculate opening balance (balance before start date)
    const openingBalance = transactions
      .filter(transaction => transaction.date < startDate)
      .reduce((balance, transaction) => {
        return transaction.type === 'thu' 
          ? balance + transaction.amount 
          : balance - transaction.amount;
      }, 0);

    // Calculate running balance for filtered transactions
    let runningBalance = openingBalance;
    const transactionsWithBalance = filteredTransactions.map(transaction => {
      runningBalance = transaction.type === 'thu' 
        ? runningBalance + transaction.amount 
        : runningBalance - transaction.amount;
      
      return {
        ...transaction,
        balance: runningBalance,
      };
    });

    // Calculate totals
    const totalIncome = filteredTransactions
      .filter(t => t.type === 'thu')
      .reduce((sum, t) => sum + t.amount, 0);
    
    const totalExpense = filteredTransactions
      .filter(t => t.type === 'chi')
      .reduce((sum, t) => sum + t.amount, 0);

    const endingBalance = openingBalance + totalIncome - totalExpense;

    return {
      openingBalance,
      totalIncome,
      totalExpense,
      endingBalance,
      transactions: transactionsWithBalance,
    };
  }, [orders, purchaseReceipts, incomeVouchers, expenseVouchers, suppliers, startDate, endDate]);

  // Filter transactions by type and payment method
  const filteredTransactions = useMemo(() => {
    let filtered = cashBookData.transactions;
    
    // Filter by transaction type
    if (filterType !== "all") {
      filtered = filtered.filter(t => t.type === filterType);
    }
    
    // Filter by payment method
    if (paymentMethodFilter !== "all") {
      filtered = filtered.filter(t => {
        // For orders, check paymentMethod field
        if (t.id.startsWith('ORDER-')) {
          const order = orders.find(o => o.id.toString() === t.id.replace('ORDER-', ''));
          return order?.paymentMethod === paymentMethodFilter;
        }
        // For vouchers, we can add payment method filtering later if needed
        return true;
      });
    }
    
    return filtered;
  }, [cashBookData.transactions, filterType, paymentMethodFilter, orders]);

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('vi-VN', {
      style: 'currency',
      currency: 'VND',
    }).format(amount);
  };

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleDateString('vi-VN', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
    });
  };

  // Handle clicking on transaction rows
  const handleTransactionClick = (transaction: CashTransaction) => {
    if (transaction.id.startsWith('INCOME-VOUCHER-')) {
      // Find the income voucher
      const voucherId = transaction.id.replace('INCOME-VOUCHER-', '');
      const voucher = incomeVouchers.find(v => v.id.toString() === voucherId);
      if (voucher) {
        setSelectedVoucher(voucher);
        setVoucherMode("edit");
        setShowIncomeVoucherModal(true);
      }
    } else if (transaction.id.startsWith('EXPENSE-VOUCHER-')) {
      // Find the expense voucher
      const voucherId = transaction.id.replace('EXPENSE-VOUCHER-', '');
      const voucher = expenseVouchers.find(v => v.id.toString() === voucherId);
      if (voucher) {
        setSelectedVoucher(voucher);
        setVoucherMode("edit");
        setShowExpenseVoucherModal(true);
      }
    }
  };

  // Handle closing modals
  const handleCloseIncomeModal = () => {
    setShowIncomeVoucherModal(false);
    setSelectedVoucher(null);
    setVoucherMode("create");
  };

  const handleCloseExpenseModal = () => {
    setShowExpenseVoucherModal(false);
    setSelectedVoucher(null);
    setVoucherMode("create");
  };

  return (
    <div className="min-h-screen bg-green-50 grocery-bg">
      <POSHeader />
      <RightSidebar />
      <div className="main-content pt-16 px-6">
        <div className="max-w-7xl mx-auto py-8">
          {/* Page Header */}
          <div className="flex justify-between items-center mb-8">
            <div>
              <h1 className="text-3xl font-bold text-gray-900 flex items-center gap-3">
                <BookOpen className="w-8 h-8 text-green-600" />
                {t('common.cashManagement')}
              </h1>
              <p className="text-gray-600 mt-2">Quản lý thu chi tiền mặt</p>
            </div>
            <div className="flex gap-3">
              <Button
                onClick={() => {
                  setSelectedVoucher(null);
                  setVoucherMode("create");
                  setShowIncomeVoucherModal(true);
                }}
                className="bg-green-600 hover:bg-green-700 text-white"
              >
                <Plus className="w-4 h-4 mr-2" />
                Lập phiếu thu
              </Button>
              <Button
                onClick={() => {
                  setSelectedVoucher(null);
                  setVoucherMode("create");
                  setShowExpenseVoucherModal(true);
                }}
                className="bg-red-600 hover:bg-red-700 text-white"
              >
                <Minus className="w-4 h-4 mr-2" />
                Lập phiếu chi
              </Button>
              <Button
                onClick={() => {
                  // Prepare export data
                  const exportData = filteredTransactions.map(transaction => ({
                    'Mã phiếu': transaction.id,
                    'Thời gian': formatDate(transaction.date),
                    'Loại thu chi': transaction.description,
                    'Người nộp/nhận': transaction.source,
                    'Thu': transaction.type === 'thu' ? formatCurrency(transaction.amount) : '',
                    'Chi': transaction.type === 'chi' ? formatCurrency(transaction.amount) : '',
                    'Tồn quỹ': formatCurrency(transaction.balance)
                  }));

                  // Create summary data
                  const summaryData = [
                    ['BÁO CÁO SỔ QUỸ TIỀN MẶT', '', '', '', '', '', ''],
                    [`Từ ngày: ${formatDate(startDate)}`, `Đến ngày: ${formatDate(endDate)}`, '', '', '', '', ''],
                    ['', '', '', '', '', '', ''],
                    ['TỔNG KẾT:', '', '', '', '', '', ''],
                    ['Quỹ đầu kỳ:', '', '', '', '', '', formatCurrency(cashBookData.openingBalance)],
                    ['Tổng thu:', '', '', '', '', formatCurrency(cashBookData.totalIncome), ''],
                    ['Tổng chi:', '', '', '', '', formatCurrency(cashBookData.totalExpense), ''],
                    ['Tồn quỹ:', '', '', '', '', '', formatCurrency(cashBookData.endingBalance)],
                    ['', '', '', '', '', '', ''],
                    ['CHI TIẾT GIAO DỊCH:', '', '', '', '', '', ''],
                    ['', '', '', '', '', '', '']
                  ];

                  // Create worksheet
                  const ws = XLSX.utils.aoa_to_sheet(summaryData);
                  
                  // Add transaction data
                  XLSX.utils.sheet_add_json(ws, exportData, {
                    origin: `A${summaryData.length + 1}`,
                    skipHeader: false
                  });

                  // Set column widths
                  const colWidths = [
                    { wch: 25 }, // Mã phiếu
                    { wch: 15 }, // Thời gian  
                    { wch: 30 }, // Loại thu chi
                    { wch: 25 }, // Người nộp/nhận
                    { wch: 15 }, // Thu
                    { wch: 15 }, // Chi
                    { wch: 15 }  // Tồn quỹ
                  ];
                  ws['!cols'] = colWidths;

                  // Style the worksheet
                  const range = XLSX.utils.decode_range(ws['!ref'] || 'A1');
                  
                  // Style header rows
                  for (let row = 0; row < 3; row++) {
                    for (let col = 0; col <= 6; col++) {
                      const cellAddress = XLSX.utils.encode_cell({ r: row, c: col });
                      if (ws[cellAddress]) {
                        ws[cellAddress].s = {
                          font: { 
                            bold: row === 0,
                            name: "Times New Roman", 
                            sz: row === 0 ? 14 : 11,
                            color: { rgb: "000000" }
                          },
                          alignment: { horizontal: "center", vertical: "center" }
                        };
                      }
                    }
                  }

                  // Style summary section
                  for (let row = 3; row < 9; row++) {
                    for (let col = 0; col <= 6; col++) {
                      const cellAddress = XLSX.utils.encode_cell({ r: row, c: col });
                      if (ws[cellAddress]) {
                        ws[cellAddress].s = {
                          font: { 
                            bold: true,
                            name: "Times New Roman", 
                            sz: 11,
                            color: { rgb: "000000" }
                          },
                          fill: { patternType: "solid", fgColor: { rgb: "E8F5E8" } },
                          alignment: { horizontal: "left", vertical: "center" }
                        };
                      }
                    }
                  }

                  // Style transaction header
                  const headerRow = summaryData.length;
                  for (let col = 0; col <= 6; col++) {
                    const cellAddress = XLSX.utils.encode_cell({ r: headerRow, c: col });
                    if (ws[cellAddress]) {
                      ws[cellAddress].s = {
                        font: { 
                          bold: true,
                          name: "Times New Roman", 
                          sz: 11,
                          color: { rgb: "FFFFFF" }
                        },
                        fill: { patternType: "solid", fgColor: { rgb: "059669" } },
                        alignment: { horizontal: "center", vertical: "center" },
                        border: {
                          top: { style: "thin", color: { rgb: "000000" } },
                          bottom: { style: "thin", color: { rgb: "000000" } },
                          left: { style: "thin", color: { rgb: "000000" } },
                          right: { style: "thin", color: { rgb: "000000" } }
                        }
                      };
                    }
                  }

                  // Style transaction data rows
                  for (let row = headerRow + 1; row <= range.e.r; row++) {
                    const isEven = (row - headerRow - 1) % 2 === 0;
                    const bgColor = isEven ? "FFFFFF" : "F8F9FA";
                    
                    for (let col = 0; col <= 6; col++) {
                      const cellAddress = XLSX.utils.encode_cell({ r: row, c: col });
                      if (ws[cellAddress]) {
                        const isCurrency = [4, 5, 6].includes(col);
                        ws[cellAddress].s = {
                          font: { 
                            name: "Times New Roman", 
                            sz: 10,
                            color: { rgb: "000000" }
                          },
                          fill: { patternType: "solid", fgColor: { rgb: bgColor } },
                          alignment: { 
                            horizontal: isCurrency ? "right" : "left", 
                            vertical: "center" 
                          },
                          border: {
                            top: { style: "thin", color: { rgb: "CCCCCC" } },
                            bottom: { style: "thin", color: { rgb: "CCCCCC" } },
                            left: { style: "thin", color: { rgb: "CCCCCC" } },
                            right: { style: "thin", color: { rgb: "CCCCCC" } }
                          }
                        };
                      }
                    }
                  }

                  // Create workbook and save
                  const wb = XLSX.utils.book_new();
                  XLSX.utils.book_append_sheet(wb, ws, 'Sổ quỹ tiền mặt');

                  // Set workbook properties
                  wb.Props = {
                    Title: 'Báo cáo sổ quỹ tiền mặt',
                    Subject: 'Chi tiết thu chi tiền mặt',
                    Author: 'EDPOS System',
                    CreatedDate: new Date()
                  };

                  // Generate filename with timestamp
                  const timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, "-");
                  const filename = `so-quy-tien-mat_${formatDate(startDate).replace(/\//g, '-')}_${formatDate(endDate).replace(/\//g, '-')}_${timestamp}.xlsx`;

                  // Save file
                  try {
                    XLSX.writeFile(wb, filename, {
                      bookType: "xlsx",
                      cellStyles: true,
                      sheetStubs: false,
                      compression: true
                    });
                    console.log("✅ Excel file exported successfully:", filename);
                  } catch (error) {
                    console.error("❌ Error exporting Excel file:", error);
                    // Fallback export without styling
                    XLSX.writeFile(wb, filename, { bookType: "xlsx" });
                  }
                }}
                variant="outline"
                className="border-green-600 text-green-600 hover:bg-green-50"
              >
                <FileText className="w-4 h-4 mr-2" />
                Xuất Excel
              </Button>
            </div>
          </div>

          {/* Filters */}
          <Card className="mb-8 border-green-200">
            <CardContent className="pt-6">
              <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
                {/* Filter Type */}
                <div>
                  <Label className="text-sm font-medium">Loại giao dịch</Label>
                  <RadioGroup value={filterType} onValueChange={setFilterType} className="mt-2">
                    <div className="flex items-center space-x-2">
                      <RadioGroupItem value="all" id="all" />
                      <Label htmlFor="all">Tất cả</Label>
                    </div>
                    <div className="flex items-center space-x-2">
                      <RadioGroupItem value="thu" id="thu" />
                      <Label htmlFor="thu">Thu</Label>
                    </div>
                    <div className="flex items-center space-x-2">
                      <RadioGroupItem value="chi" id="chi" />
                      <Label htmlFor="chi">Chi</Label>
                    </div>
                  </RadioGroup>
                </div>

                {/* Payment Method Filter */}
                <div>
                  <Label className="text-sm font-medium">Loại quỹ</Label>
                  <Select value={paymentMethodFilter} onValueChange={setPaymentMethodFilter}>
                    <SelectTrigger className="mt-2">
                      <SelectValue placeholder="Chọn loại quỹ" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">Tất cả</SelectItem>
                      <SelectItem value="cash">Tiền mặt</SelectItem>
                      <SelectItem value="transfer">Chuyển khoản</SelectItem>
                      <SelectItem value="momo">MoMo</SelectItem>
                      <SelectItem value="zalopay">ZaloPay</SelectItem>
                      <SelectItem value="vnpay">VNPay</SelectItem>
                      <SelectItem value="qrCode">QR Code</SelectItem>
                      <SelectItem value="creditCard">Thẻ tín dụng</SelectItem>
                      <SelectItem value="debitCard">Thẻ ghi nợ</SelectItem>
                      {paymentMethods.filter(pm => pm.enabled).map((method) => (
                        <SelectItem key={method.id} value={method.code}>
                          {method.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                {/* Start Date */}
                <div>
                  <Label className="text-sm font-medium">Từ ngày</Label>
                  <Input
                    type="date"
                    value={startDate}
                    onChange={(e) => setStartDate(e.target.value)}
                    className="mt-2"
                  />
                </div>

                {/* End Date */}
                <div>
                  <Label className="text-sm font-medium">Đến ngày</Label>
                  <Input
                    type="date"
                    value={endDate}
                    onChange={(e) => setEndDate(e.target.value)}
                    className="mt-2"
                  />
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Summary Cards */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
            <Card className="border-blue-200">
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium text-gray-600">
                  Quỹ đầu kỳ
                </CardTitle>
                <Wallet className="h-4 w-4 text-blue-600" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-blue-600">
                  {formatCurrency(cashBookData.openingBalance)}
                </div>
              </CardContent>
            </Card>

            <Card className="border-green-200">
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium text-gray-600">
                  Tổng thu
                </CardTitle>
                <TrendingUp className="h-4 w-4 text-green-600" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-green-600">
                  {formatCurrency(cashBookData.totalIncome)}
                </div>
              </CardContent>
            </Card>

            <Card className="border-red-200">
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium text-gray-600">
                  Tổng chi
                </CardTitle>
                <TrendingDown className="h-4 w-4 text-red-600" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-red-600">
                  -{formatCurrency(cashBookData.totalExpense)}
                </div>
              </CardContent>
            </Card>

            <Card className="border-purple-200">
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium text-gray-600">
                  Tồn quỹ
                </CardTitle>
                <Wallet className="h-4 w-4 text-purple-600" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-purple-600">
                  {formatCurrency(cashBookData.endingBalance)}
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Transactions Table */}
          <Card className="border-green-200">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <FileText className="w-5 h-5" />
                Chi tiết giao dịch
              </CardTitle>
            </CardHeader>
            <CardContent>
              {filteredTransactions.length === 0 ? (
                <div className="text-center py-12">
                  <FileText className="w-16 h-16 text-gray-300 mx-auto mb-4" />
                  <p className="text-gray-600">Không có giao dịch nào trong khoảng thời gian này</p>
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-[120px]">Mã phiếu</TableHead>
                        <TableHead className="w-[120px]">Thời gian</TableHead>
                        <TableHead>Loại thu chi</TableHead>
                        <TableHead>Người nộp/nhận</TableHead>
                        <TableHead className="text-center w-[80px]">Thu</TableHead>
                        <TableHead className="text-center w-[80px]">Chi</TableHead>
                        <TableHead className="text-right w-[120px]">Tồn</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {filteredTransactions.map((transaction) => (
                        <TableRow 
                          key={transaction.id}
                          className={
                            (transaction.id.startsWith('INCOME-VOUCHER-') || transaction.id.startsWith('EXPENSE-VOUCHER-'))
                              ? "cursor-pointer hover:bg-gray-50" 
                              : ""
                          }
                          onClick={() => handleTransactionClick(transaction)}
                        >
                          <TableCell className="font-medium">
                            {transaction.id}
                          </TableCell>
                          <TableCell>
                            <div className="flex items-center gap-2">
                              <Calendar className="w-4 h-4 text-gray-400" />
                              {formatDate(transaction.date)}
                            </div>
                          </TableCell>
                          <TableCell>
                            <div className="flex items-center gap-2">
                              {transaction.type === 'thu' ? (
                                <>
                                  <Plus className="w-4 h-4 text-green-500" />
                                  <Badge className="bg-green-100 text-green-800">
                                    {transaction.description}
                                  </Badge>
                                </>
                              ) : (
                                <>
                                  <Minus className="w-4 h-4 text-red-500" />
                                  <Badge className="bg-red-100 text-red-800">
                                    {transaction.description}
                                  </Badge>
                                </>
                              )}
                            </div>
                          </TableCell>
                          <TableCell>{transaction.source}</TableCell>
                          <TableCell className="text-center">
                            {transaction.type === 'thu' ? (
                              <span className="text-green-600 font-medium">
                                {formatCurrency(transaction.amount)}
                              </span>
                            ) : (
                              <span className="text-gray-400">-</span>
                            )}
                          </TableCell>
                          <TableCell className="text-center">
                            {transaction.type === 'chi' ? (
                              <span className="text-red-600 font-medium">
                                {formatCurrency(transaction.amount)}
                              </span>
                            ) : (
                              <span className="text-gray-400">-</span>
                            )}
                          </TableCell>
                          <TableCell className="text-right font-medium">
                            {formatCurrency(transaction.balance)}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Income Voucher Modal */}
      <IncomeVoucherModal
        isOpen={showIncomeVoucherModal}
        onClose={handleCloseIncomeModal}
        voucher={selectedVoucher}
        mode={voucherMode}
      />

      {/* Expense Voucher Modal */}
      <ExpenseVoucherModal
        isOpen={showExpenseVoucherModal}
        onClose={handleCloseExpenseModal}
        voucher={selectedVoucher}
        mode={voucherMode}
      />
    </div>
  );
}
